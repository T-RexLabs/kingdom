package sources

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"kingdom/internal/central/catalog"
	_ "modernc.org/sqlite"
)

func fixture(t *testing.T) (*Service, *catalog.Store, catalog.User, catalog.Project) {
	t.Helper()
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	identity := catalog.Identity{Issuer: "https://issuer.example", Subject: "owner"}
	authority, err := catalog.New(ctx, db, catalog.Config{InitialOwners: []catalog.Identity{identity}})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := authority.BootstrapOwner(ctx, identity, "Owner")
	if err != nil {
		t.Fatal(err)
	}
	project, err := authority.CreateProject(ctx, owner.ID, "Project")
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(ctx, db, authority, filepath.Join(t.TempDir(), "sources"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { service.Close() })
	return service, authority, owner, project
}
func archive(t *testing.T, names ...string) []byte {
	t.Helper()
	var out bytes.Buffer
	w := zip.NewWriter(&out)
	for _, name := range names {
		f, e := w.Create(name)
		if e != nil {
			t.Fatal(e)
		}
		f.Write([]byte("content:" + name))
	}
	if e := w.Close(); e != nil {
		t.Fatal(e)
	}
	return out.Bytes()
}
func TestInitialSourceReplayAndStartupFence(t *testing.T) {
	s, _, owner, project := fixture(t)
	ctx := context.Background()
	key := uuid.NewString()
	raw := archive(t, "src/a.txt", "b.txt")
	result, err := s.Upload(ctx, owner.ID, project.ID, key, "zip", "", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if result.Files != 2 {
		t.Fatal(result)
	}
	replay, err := s.Upload(ctx, owner.ID, project.ID, key, "zip", "", bytes.NewReader(raw))
	if err != nil || replay != result {
		t.Fatal(replay, err)
	}
	if _, err = s.Upload(ctx, owner.ID, project.ID, key, "file", "other", strings.NewReader("changed")); err == nil {
		t.Fatal("different retry accepted")
	}
	destination := t.TempDir()
	if err = s.PrepareInitial(ctx, project.ID, destination); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(destination, "src/a.txt"))
	if err != nil || string(content) != "content:src/a.txt" {
		t.Fatal(string(content), err)
	}
	if _, err = s.Upload(ctx, owner.ID, project.ID, uuid.NewString(), "file", "new", strings.NewReader("new")); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	replay, err = s.Upload(ctx, owner.ID, project.ID, key, "zip", "", bytes.NewReader(raw))
	if err != nil || replay != result {
		t.Fatal(replay, err)
	}
	os.WriteFile(filepath.Join(destination, "b.txt"), []byte("runtime result"), 0644)
	restarted, err := New(ctx, s.db, s.catalog, s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err = restarted.PrepareInitial(ctx, project.ID, destination); err != nil {
		t.Fatal(err)
	}
	content, _ = os.ReadFile(filepath.Join(destination, "b.txt"))
	if string(content) != "runtime result" {
		t.Fatal("startup overwrote runtime result")
	}
	status, err := s.Status(ctx, owner.ID, project.ID)
	if err != nil || status.State != "started" {
		t.Fatal(status, err)
	}
}
func TestEmptyStartupPermanentlyExcludesUpload(t *testing.T) {
	s, _, owner, project := fixture(t)
	ctx := context.Background()
	dest := t.TempDir()
	if err := s.PrepareInitial(ctx, project.ID, dest); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Upload(ctx, owner.ID, project.ID, uuid.NewString(), "file", "a", strings.NewReader("a")); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err := s.PrepareInitial(ctx, project.ID, t.TempDir()); !errors.Is(err, ErrConflict) {
		t.Fatal("replacement root accepted", err)
	}
}
func TestSourceRejectsUnsafeInputAndForeignActor(t *testing.T) {
	s, authority, owner, project := fixture(t)
	ctx := context.Background()
	member, err := authority.CreateUser(ctx, owner.ID, catalog.Identity{Issuer: "https://issuer.example", Subject: "member"}, "Member")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Upload(ctx, member.ID, project.ID, uuid.NewString(), "file", "a", strings.NewReader("a")); err == nil {
		t.Fatal("foreign actor uploaded")
	}
	for _, name := range []string{"../escape", "/absolute", ".git/config", "a/../../escape", "a\\b"} {
		if _, err = s.Upload(ctx, owner.ID, project.ID, uuid.NewString(), "zip", "", bytes.NewReader(archive(t, name))); err == nil {
			t.Fatal("unsafe input accepted", name)
		}
	}
	if _, err = s.Upload(ctx, owner.ID, project.ID, uuid.NewString(), "file", "src/a.txt", strings.NewReader("file")); err != nil {
		t.Fatal(err)
	}
}

type blockedInput struct {
	entered, release chan struct{}
	data             *strings.Reader
}

func (r *blockedInput) Read(p []byte) (int, error) {
	if r.entered != nil {
		close(r.entered)
		<-r.release
		r.entered = nil
	}
	return r.data.Read(p)
}
func TestStartupExcludesAnUploadAlreadyBeingRead(t *testing.T) {
	s, _, owner, project := fixture(t)
	ctx := context.Background()
	reader := &blockedInput{entered: make(chan struct{}), release: make(chan struct{}), data: strings.NewReader("input")}
	entered := reader.entered
	done := make(chan error, 1)
	go func() {
		_, err := s.Upload(ctx, owner.ID, project.ID, uuid.NewString(), "file", "a", reader)
		done <- err
	}()
	<-entered
	if err := s.PrepareInitial(ctx, project.ID, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	close(reader.release)
	if err := <-done; !errors.Is(err, ErrConflict) {
		t.Fatal("in-flight upload escaped startup fence", err)
	}
}
func TestFailedInitialCopyReplaysWithoutOverwrite(t *testing.T) {
	s, _, owner, project := fixture(t)
	ctx := context.Background()
	if _, err := s.Upload(ctx, owner.ID, project.ID, uuid.NewString(), "zip", "", bytes.NewReader(archive(t, "a", "b"))); err != nil {
		t.Fatal(err)
	}
	var stage string
	if err := s.db.QueryRow(`SELECT stage FROM central_project_sources WHERE project=?`, project.ID).Scan(&stage); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(s.path, stage, "b")
	if err := os.WriteFile(input, []byte("corrupted"), 0644); err != nil {
		t.Fatal(err)
	}
	destination := t.TempDir()
	if err := s.PrepareInitial(ctx, project.ID, destination); err == nil {
		t.Fatal("corrupt source accepted")
	}
	if content, err := os.ReadFile(filepath.Join(destination, "a")); err != nil || string(content) != "content:a" {
		t.Fatal("expected retained partial copy", err)
	}
	if _, err := s.Upload(ctx, owner.ID, project.ID, uuid.NewString(), "file", "other", strings.NewReader("other")); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, []byte("content:b"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareInitial(ctx, project.ID, destination); err != nil {
		t.Fatal(err)
	}
}
func TestInitialStartupRejectsUntrackedNonemptyRoot(t *testing.T) {
	s, _, _, project := fixture(t)
	destination := t.TempDir()
	os.WriteFile(filepath.Join(destination, "foreign"), []byte("foreign"), 0600)
	if err := s.PrepareInitial(context.Background(), project.ID, destination); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
}

func TestUploadRechecksOwnerRevocationAfterBody(t *testing.T) {
	s, authority, owner, project := fixture(t)
	ctx := context.Background()
	second, err := authority.CreateUser(ctx, owner.ID, catalog.Identity{Issuer: "https://issuer.example", Subject: "second"}, "Second")
	if err != nil {
		t.Fatal(err)
	}
	if err = authority.SetMembership(ctx, owner.ID, project.ID, second.ID, catalog.Owner); err != nil {
		t.Fatal(err)
	}
	reader := &blockedInput{entered: make(chan struct{}), release: make(chan struct{}), data: strings.NewReader("input")}
	entered := reader.entered
	done := make(chan error, 1)
	go func() {
		_, err := s.Upload(ctx, second.ID, project.ID, uuid.NewString(), "file", "a", reader)
		done <- err
	}()
	<-entered
	if err = authority.RevokeMembership(ctx, owner.ID, project.ID, second.ID); err != nil {
		t.Fatal(err)
	}
	close(reader.release)
	if err = <-done; !errors.Is(err, catalog.ErrDenied) {
		t.Fatal("revoked actor published source", err)
	}
	status, err := s.Status(ctx, owner.ID, project.ID)
	if err != nil || status.State != "empty" || !status.CanUpload {
		t.Fatal(status, err)
	}
}

func TestRepositorySourceProvenanceIsImmutable(t *testing.T) {
	s, _, owner, project := fixture(t)
	ctx := context.Background()
	key := uuid.NewString()
	data := archive(t, "main.go")
	origin := RepositoryOrigin{RepositoryID: 42, CommitSHA: strings.Repeat("a", 40), TreeSHA: strings.Repeat("b", 40)}
	result, err := s.ImportRepository(ctx, owner.ID, project.ID, key, origin, bytes.NewReader(data))
	if err != nil || result.Repository == nil || *result.Repository != origin || result.Kind != "github" {
		t.Fatalf("provenance: %+v %v", result, err)
	}
	replay, err := s.ImportRepository(ctx, owner.ID, project.ID, key, origin, bytes.NewReader(data))
	if err != nil || replay.ID != result.ID {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	origin.CommitSHA = strings.Repeat("c", 40)
	if _, err = s.ImportRepository(ctx, owner.ID, project.ID, key, origin, bytes.NewReader(data)); err == nil {
		t.Fatal("changed provenance accepted")
	}
	if _, err = s.Upload(ctx, owner.ID, project.ID, uuid.NewString(), "github", "", bytes.NewReader(data)); err == nil {
		t.Fatal("public upload supplied repository provenance")
	}
}
