//go:build linux || darwin

package workspacetransfer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"kingdom/internal/securefs"
	"kingdom/internal/workspacearchive"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type pipeListener struct {
	connections chan net.Conn
	done        chan struct{}
	once        sync.Once
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connections:
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *pipeListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *pipeListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4097} }

type helperHarness struct {
	client *http.Client
	output bytes.Buffer
	result chan error
	cancel context.CancelFunc
	job    Job
}

func startHelper(t *testing.T, root string, job Job) *helperHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	listener := &pipeListener{connections: make(chan net.Conn), done: make(chan struct{})}
	started := make(chan struct{})
	h := &helperHarness{result: make(chan error, 1), cancel: cancel, job: job}
	transport := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		a, b := net.Pipe()
		select {
		case listener.connections <- b:
			return a, nil
		case <-ctx.Done():
			a.Close()
			b.Close()
			return nil, ctx.Err()
		case <-listener.done:
			a.Close()
			b.Close()
			return nil, net.ErrClosed
		}
	}}
	h.client = &http.Client{Transport: transport}
	go func() {
		h.result <- serve(ctx, job, root, &h.output, func() (net.Listener, error) { close(started); return listener, nil })
	}()
	select {
	case <-started:
	case err := <-h.result:
		t.Fatalf("helper startup: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("helper startup blocked")
	}
	t.Cleanup(func() { cancel(); listener.Close(); transport.CloseIdleConnections() })
	return h
}
func (h *helperHarness) request(method, path string, body io.Reader, size int64) (*http.Response, error) {
	req, _ := http.NewRequest(method, "http://managed.invalid"+path, body)
	req.Header.Set("Authorization", "Bearer "+h.job.Nonce)
	if body != nil {
		req.ContentLength = size
	}
	return h.client.Do(req)
}
func (h *helperHarness) finish(t *testing.T) error {
	t.Helper()
	select {
	case err := <-h.result:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("helper did not finish")
		return nil
	}
}
func testJob(mode string) Job {
	return Job{Version: 1, Mode: mode, Nonce: strings.Repeat("a", 64), HelperSHA256: strings.Repeat("b", 64), TimeoutSeconds: 5, Limits: workspacearchive.Limits{ArchiveBytes: 8 << 20, ExpandedBytes: 16 << 20, FileBytes: 4 << 20, Entries: 100, Depth: 16, PathBytes: 256}}
}
func testZIP(t *testing.T) ([]byte, []byte) {
	t.Helper()
	root := t.TempDir()
	content := make([]byte, 2<<20)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "source.bin"), content, 0755); err != nil {
		t.Fatal(err)
	}
	pinned, err := securefs.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	var archive bytes.Buffer
	if _, err := workspacearchive.ExportZIP(context.Background(), pinned, &archive, testJob("export").Limits); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes(), content
}
func importJob(data []byte) Job {
	j := testJob("import")
	hash := sha256.Sum256(data)
	j.ArchiveSHA256 = hex.EncodeToString(hash[:])
	j.ArchiveBytes = int64(len(data))
	return j
}

func TestStreamingImportExportBeyondManagedCommandLimit(t *testing.T) {
	data, content := testZIP(t)
	if len(data) <= 1<<20 {
		t.Fatal("fixture does not exceed managed stdin limit")
	}
	root := t.TempDir()
	job := importJob(data)
	h := startHelper(t, root, job)
	response, err := h.client.Get("http://managed.invalid/v1/ready")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 401 {
		t.Fatal("unauthenticated readiness accepted")
	}
	response, err = h.request("GET", "/v1/ready", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 204 {
		t.Fatal(response.Status)
	}
	response, err = h.request("PUT", "/v1/archive", bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 204 {
		t.Fatalf("%s: %v", response.Status, h.finish(t))
	}
	if err := h.finish(t); err != nil {
		t.Fatal(err)
	}
	stored, err := os.ReadFile(filepath.Join(root, "project/source.bin"))
	if err != nil || !bytes.Equal(stored, content) {
		t.Fatal("import changed content")
	}
	expected, err := workspacearchive.ImportZIP(context.Background(), t.TempDir(), bytes.NewReader(data), int64(len(data)), job.Limits)
	if err != nil {
		t.Fatal(err)
	}
	defer expected.Close()
	if err := ValidateImport(bytes.NewReader(h.output.Bytes()), job, expected); err != nil {
		t.Fatal(err)
	}
	if h.output.Len() > MaxCompletionBytes {
		t.Fatal("completion exceeded managed output bound")
	}
	exported := startHelper(t, root, testJob("export"))
	response, err = exported.request("GET", "/v1/archive", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	download, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := exported.finish(t); err != nil {
		t.Fatal(err)
	}
	staged, err := ValidateExport(context.Background(), t.TempDir(), bytes.NewReader(download), int64(len(download)), bytes.NewReader(exported.output.Bytes()), exported.job)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Close()
	if !bytes.Equal(download, data) {
		t.Fatal("roundtrip export changed deterministic ZIP")
	}
	var completion Completion
	if err := json.Unmarshal(exported.output.Bytes(), &completion); err != nil {
		t.Fatal(err)
	}
	completion.ManifestSHA256 = strings.Repeat("0", 64)
	bad, _ := json.Marshal(completion)
	if staged, err := ValidateExport(context.Background(), t.TempDir(), bytes.NewReader(download), int64(len(download)), bytes.NewReader(bad), exported.job); err == nil {
		staged.Close()
		t.Fatal("forged manifest accepted")
	}
}
func TestImportRejectsWrongDigestAndExistingDestination(t *testing.T) {
	data, _ := testZIP(t)
	for _, mode := range []string{"digest", "existing"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			job := importJob(data)
			if mode == "digest" {
				job.ArchiveSHA256 = strings.Repeat("0", 64)
			} else {
				os.Mkdir(filepath.Join(root, "project"), 0700)
				os.WriteFile(filepath.Join(root, "project/retained"), []byte("retained"), 0600)
			}
			h := startHelper(t, root, job)
			response, err := h.request("PUT", "/v1/archive", bytes.NewReader(data), int64(len(data)))
			if err == nil {
				response.Body.Close()
			}
			if err := h.finish(t); err == nil || h.output.Len() != 0 {
				t.Fatal("failed import emitted completion")
			}
			if mode == "existing" {
				if b, err := os.ReadFile(filepath.Join(root, "project/retained")); err != nil || string(b) != "retained" {
					t.Fatal("existing project replaced")
				}
			}
		})
	}
}
func TestImportDisconnectAndCancellationEmitNoCompletion(t *testing.T) {
	data, _ := testZIP(t)
	root := t.TempDir()
	h := startHelper(t, root, importJob(data))
	input, output := io.Pipe()
	result := make(chan error, 1)
	go func() {
		response, err := h.request("PUT", "/v1/archive", input, int64(len(data)))
		if response != nil {
			response.Body.Close()
		}
		result <- err
	}()
	output.Write(data[:1024])
	output.CloseWithError(errors.New("connection lost"))
	<-result
	if err := h.finish(t); err == nil || h.output.Len() != 0 {
		t.Fatal("interrupted import emitted completion")
	}
	if _, err := os.Stat(filepath.Join(root, "project")); !os.IsNotExist(err) {
		t.Fatal("partial import published")
	}
}
func TestExportDisconnectEmitsNoCompletion(t *testing.T) {
	data, content := testZIP(t)
	_ = data
	root := t.TempDir()
	os.Mkdir(filepath.Join(root, "project"), 0700)
	os.WriteFile(filepath.Join(root, "project/file"), content, 0600)
	h := startHelper(t, root, testJob("export"))
	response, err := h.request("GET", "/v1/archive", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if err := h.finish(t); err == nil || h.output.Len() != 0 {
		t.Fatal("disconnected export emitted completion")
	}
}
func TestJobValidationAndFixedCommand(t *testing.T) {
	job := testJob("export")
	command, err := Command(job)
	if err != nil {
		t.Fatal(err)
	}
	if len(command.Argv) != 2 || command.Argv[0] != HelperPath || command.Argv[1] != "serve-v1" || command.Workdir != WorkspaceParent || len(command.Stdin) > MaxJobBytes || command.OutputLimitBytes != MaxCompletionBytes {
		t.Fatal("command escaped fixed protocol")
	}
	if _, err := DecodeJob(bytes.NewReader(command.Stdin)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{append(append([]byte{}, command.Stdin...), []byte(`{}`)...), bytes.Replace(command.Stdin, []byte(`"version":1`), []byte(`"version":1,"unknown":true`), 1), bytes.Repeat([]byte(" "), MaxJobBytes+1)} {
		if _, err := DecodeJob(bytes.NewReader(raw)); err == nil {
			t.Fatal("invalid metadata accepted")
		}
	}
	job.Limits.ArchiveBytes = 1 << 62
	if job.Validate() == nil {
		t.Fatal("overflow limit accepted")
	}
	if strings.Contains(fmt.Sprintf("%+v %#v", job, job), job.Nonce) {
		t.Fatal("capability leaked through formatting")
	}
}

func TestTransferOneShotClaimAndTimeout(t *testing.T) {
	data, _ := testZIP(t)
	h := startHelper(t, t.TempDir(), importJob(data))
	reader, writer := io.Pipe()
	first := make(chan error, 1)
	go func() {
		response, err := h.request("PUT", "/v1/archive", reader, int64(len(data)))
		if response != nil {
			response.Body.Close()
			if response.StatusCode != 204 {
				err = fmt.Errorf("first transfer status %d", response.StatusCode)
			}
		}
		first <- err
	}()
	if _, err := writer.Write(data[:1]); err != nil {
		t.Fatal(err)
	}
	// Client consumption of the first byte does not prove server admission.
	deadline := time.Now().Add(2 * time.Second)
	for {
		ready, err := h.request("GET", "/v1/ready", nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ready.Body.Close()
		if ready.StatusCode == http.StatusConflict {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first transfer was not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	response, err := h.request("PUT", "/v1/archive", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 409 {
		t.Fatalf("duplicate transfer admitted: %s", response.Status)
	}
	if _, err := writer.Write(data[1:]); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := h.finish(t); err != nil {
		t.Fatal(err)
	}
	job := importJob(data)
	job.TimeoutSeconds = 1
	idle := startHelper(t, t.TempDir(), job)
	if err := idle.finish(t); !errors.Is(err, context.DeadlineExceeded) || idle.output.Len() != 0 {
		t.Fatalf("idle helper did not expire without completion: %v", err)
	}
}
