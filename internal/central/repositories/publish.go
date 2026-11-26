package repositories

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/mutation"
	"kingdom/internal/publishedresult"
)

type PublishRequest struct {
	RunID      string `json:"run_id"`
	BaseBranch string `json:"base_branch"`
	Title      string `json:"title"`
	Body       string `json:"body"`
}
type Publication struct {
	ID          string `json:"id,omitempty"`
	State       string `json:"state"`
	Branch      string `json:"branch,omitempty"`
	PullRequest int64  `json:"pull_request,omitempty"`
	URL         string `json:"url,omitempty"`
}
type publishSpec struct {
	Request    PublishRequest
	Grant      Grant
	Snapshot   publishedresult.ResultSnapshot
	BaseCommit string
	Created    time.Time
}
type publishRecord struct {
	ID      string
	Spec    publishSpec
	Commit  string
	PRState string
	Number  int64
}

func publicationSchema(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS central_repository_publications(actor TEXT NOT NULL,request_key TEXT NOT NULL,id TEXT NOT NULL UNIQUE,project TEXT NOT NULL REFERENCES central_projects(id),fingerprint TEXT NOT NULL,spec BLOB NOT NULL,commit_sha TEXT NOT NULL DEFAULT '',pr_state TEXT NOT NULL DEFAULT '' CHECK(pr_state IN('','pending','done')),pr_number INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(actor,request_key));
 CREATE TRIGGER IF NOT EXISTS central_repository_publication_identity BEFORE UPDATE ON central_repository_publications WHEN NEW.actor!=OLD.actor OR NEW.request_key!=OLD.request_key OR NEW.id!=OLD.id OR NEW.project!=OLD.project OR NEW.fingerprint!=OLD.fingerprint OR NEW.spec!=OLD.spec OR (OLD.commit_sha!='' AND OLD.commit_sha!=NEW.commit_sha) OR (OLD.pr_state='pending' AND NEW.pr_state='') OR (OLD.pr_state='done' AND NEW.pr_state!='done') OR (OLD.pr_number!=0 AND OLD.pr_number!=NEW.pr_number) BEGIN SELECT RAISE(ABORT,'publication is immutable');END;
 CREATE TRIGGER IF NOT EXISTS central_repository_publication_retained BEFORE DELETE ON central_repository_publications BEGIN SELECT RAISE(ABORT,'publication is retained');END;`)
	return err
}
func (s *Service) publication(ctx context.Context, actor, project, key string, request PublishRequest, candidate *publishSpec) (publishRecord, bool, error) {
	grant, ok := s.client.Grant(project)
	if !ok || !grant.Publish {
		return publishRecord{}, false, ErrDenied
	}
	fingerprint, _ := mutation.Fingerprint(struct {
		Grant   Grant
		Request PublishRequest
	}{grant, request})
	encoded := hex.EncodeToString(fingerprint[:])
	tx, err := s.write(ctx)
	if err != nil {
		return publishRecord{}, false, err
	}
	defer tx.Rollback()
	if err = catalog.AuthorizeTx(ctx, tx, actor, project, catalog.ManageProject); err != nil {
		return publishRecord{}, false, err
	}
	var record publishRecord
	var retained string
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT id,spec,commit_sha,pr_state,pr_number,fingerprint FROM central_repository_publications WHERE actor=? AND request_key=?`, actor, key).Scan(&record.ID, &raw, &record.Commit, &record.PRState, &record.Number, &retained)
	if err == nil {
		if retained != encoded {
			return record, false, mutation.ErrConflict
		}
		if json.Unmarshal(raw, &record.Spec) != nil {
			return record, false, ErrUnavailable
		}
		return record, true, nil
	}
	if err != sql.ErrNoRows {
		return record, false, err
	}
	if candidate == nil {
		return record, false, nil
	}
	var outstanding int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM central_repository_publications WHERE actor=? AND project=? AND pr_state!='done'`, actor, project).Scan(&outstanding); err != nil {
		return record, false, err
	}
	if outstanding >= 100 {
		return record, false, ErrUnavailable
	}
	record.ID = uuid.NewString()
	record.Spec = *candidate
	raw, err = json.Marshal(candidate)
	if err != nil {
		return record, false, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO central_repository_publications(actor,request_key,id,project,fingerprint,spec)VALUES(?,?,?,?,?,?)`, actor, key, record.ID, project, encoded, raw); err != nil {
		return record, false, err
	}
	if err = tx.Commit(); err != nil {
		return record, false, ErrUnavailable
	}
	return record, true, nil
}
func (s *Service) updatePublication(ctx context.Context, actor, project, id, commit, state string, number int64) (bool, error) {
	tx, err := s.write(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err = catalog.AuthorizeTx(ctx, tx, actor, project, catalog.ManageProject); err != nil {
		return false, err
	}
	var result sql.Result
	if commit != "" {
		result, err = tx.ExecContext(ctx, `UPDATE central_repository_publications SET commit_sha=? WHERE id=? AND (commit_sha='' OR commit_sha=?)`, commit, id, commit)
	} else if state == "pending" {
		result, err = tx.ExecContext(ctx, `UPDATE central_repository_publications SET pr_state='pending' WHERE id=? AND pr_state='' AND commit_sha!=''`, id)
	} else {
		result, err = tx.ExecContext(ctx, `UPDATE central_repository_publications SET pr_state='done',pr_number=? WHERE id=? AND pr_state IN('pending','done') AND (pr_number=0 OR pr_number=?)`, number, id, number)
	}
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, ErrUnavailable
	}
	return n == 1, nil
}
func (s *Service) Publish(ctx context.Context, actor, project, key string, request PublishRequest) (Publication, error) {
	if s.results == nil {
		return Publication{}, ErrUnavailable
	}
	if !mutation.ValidKey(key) || request.RunID == "" || len(request.RunID) > 256 || !validRef(request.BaseBranch) || strings.TrimSpace(request.Title) == "" || len(request.Title) > 200 || strings.ContainsAny(request.Title, "\r\n\x00") || len(request.Body) > 8192 || strings.ContainsRune(request.Body, '\x00') {
		return Publication{}, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		return Publication{}, ctx.Err()
	}
	record, found, err := s.publication(ctx, actor, project, key, request, nil)
	if err != nil {
		return Publication{}, err
	}
	authorize := func() error { return s.catalog.Authorize(ctx, actor, project, catalog.ManageProject) }
	if !found {
		snapshot, err := s.results.Resolve(ctx, actor, project, request.RunID)
		if err != nil || !validResult(snapshot, request.RunID) {
			return Publication{}, ErrUnavailable
		}
		if len(snapshot.Changes) == 0 {
			return Publication{State: "unchanged"}, nil
		}
		base, err := s.client.resolveBranch(ctx, project, request.BaseBranch)
		if err != nil {
			return Publication{}, err
		}
		grant, _ := s.client.Grant(project)
		record, _, err = s.publication(ctx, actor, project, key, request, &publishSpec{request, grant, snapshot, base, time.Now().UTC().Truncate(time.Second)})
		if err != nil {
			return Publication{}, err
		}
	}
	a, err := s.client.authorize(ctx, project, true)
	if err != nil {
		return Publication{}, err
	}
	branch := "kingdom/" + record.ID
	result := Publication{ID: record.ID, State: "pending", Branch: branch}
	if record.Commit == "" {
		tree, err := s.client.buildResultDelta(ctx, a, s.results, actor, project, record.Spec.Snapshot, record.Spec.BaseCommit, authorize)
		if err != nil {
			return result, err
		}
		if err = authorize(); err != nil {
			return result, err
		}
		commit, err := s.client.createResultCommit(ctx, a, tree, record.Spec.BaseCommit, record.Spec.Request.Title+"\n\nKingdom-Publication: "+record.ID+"\n", record.Spec.Created)
		if err != nil {
			return result, err
		}
		if ok, err := s.updatePublication(ctx, actor, project, record.ID, commit, "", 0); err != nil || !ok {
			return result, ErrUnavailable
		}
		record.Commit = commit
	}
	if err = authorize(); err != nil {
		return result, err
	}
	if err = s.client.ensureBranch(ctx, a, branch, record.Commit); err != nil {
		return result, err
	}
	marker := "<!-- kingdom-publication:" + record.ID + " -->"
	number, err := s.client.findPull(ctx, a, branch, request.BaseBranch, record.Commit, marker)
	if err != nil {
		return result, err
	}
	if number == 0 && record.PRState == "" {
		claimed, err := s.updatePublication(ctx, actor, project, record.ID, "", "pending", 0)
		if err != nil {
			return result, err
		}
		if claimed {
			if err = authorize(); err != nil {
				return result, err
			}
			var pull pullResponse
			err = s.client.request(ctx, http.MethodPost, a.path("/pulls"), a.token, map[string]string{"title": request.Title, "body": request.Body + "\n\n" + marker, "head": branch, "base": request.BaseBranch}, maxMetadataBytes, &pull)
			if err != nil {
				return result, ErrUnavailable
			}
			if !pull.matches(a, branch, request.BaseBranch, record.Commit, marker) {
				return result, ErrUnavailable
			}
			number = pull.Number
		}
	}
	if number == 0 {
		return result, nil
	}
	if ok, err := s.updatePublication(ctx, actor, project, record.ID, "", "done", number); err != nil || !ok {
		return result, ErrUnavailable
	}
	result.State = "published"
	result.PullRequest = number
	result.URL = fmt.Sprintf("https://github.com/%s/%s/pull/%d", a.repo.Owner.Login, a.repo.Name, number)
	return result, nil
}
func (c *Client) ensureBranch(ctx context.Context, a access, branch, commit string) error {
	var ref struct {
		Ref    string `json:"ref"`
		Object struct {
			SHA  string `json:"sha"`
			Type string `json:"type"`
		} `json:"object"`
	}
	err := c.request(ctx, http.MethodGet, a.path("/git/ref/heads/"+branch), a.token, nil, maxMetadataBytes, &ref)
	if err != nil {
		var status responseError
		if !errors.As(err, &status) || status != 404 {
			return err
		}
		err = c.request(ctx, http.MethodPost, a.path("/git/refs"), a.token, map[string]string{"ref": "refs/heads/" + branch, "sha": commit}, maxMetadataBytes, &ref)
		if err != nil {
			return err
		}
	}
	if ref.Ref != "refs/heads/"+branch || ref.Object.SHA != commit || ref.Object.Type != "commit" {
		return ErrDenied
	}
	return nil
}

type pullResponse struct {
	Number int64  `json:"number"`
	Body   string `json:"body"`
	Head   struct {
		Ref  string     `json:"ref"`
		SHA  string     `json:"sha"`
		Repo repository `json:"repo"`
	} `json:"head"`
	Base struct {
		Ref  string     `json:"ref"`
		Repo repository `json:"repo"`
	} `json:"base"`
}

func (p pullResponse) matches(a access, branch, base, commit, marker string) bool {
	return p.Number > 0 && p.Head.Ref == branch && p.Head.SHA == commit && p.Head.Repo.ID == a.grant.RepositoryID && p.Base.Ref == base && p.Base.Repo.ID == a.grant.RepositoryID && strings.Contains(p.Body, marker)
}
func (c *Client) findPull(ctx context.Context, a access, branch, base, commit, marker string) (int64, error) {
	query := url.Values{"state": {"all"}, "head": {a.repo.Owner.Login + ":" + branch}, "base": {base}, "per_page": {"100"}}
	var pulls []pullResponse
	if err := c.request(ctx, http.MethodGet, a.path("/pulls?")+query.Encode(), a.token, nil, maxMetadataBytes, &pulls); err != nil {
		return 0, err
	}
	if len(pulls) >= 100 {
		return 0, ErrUnavailable
	}
	var number int64
	for _, pull := range pulls {
		if !pull.matches(a, branch, base, commit, marker) || number != 0 {
			return 0, ErrUnavailable
		}
		number = pull.Number
	}
	return number, nil
}

func (c *Client) resolveBranch(ctx context.Context, project, branch string) (string, error) {
	if !validRef(branch) {
		return "", ErrInvalid
	}
	a, err := c.authorize(ctx, project, false)
	if err != nil {
		return "", err
	}
	var ref struct {
		Ref    string `json:"ref"`
		Object struct {
			SHA  string `json:"sha"`
			Type string `json:"type"`
		} `json:"object"`
	}
	if err = c.request(ctx, http.MethodGet, a.path("/git/ref/heads/"+branch), a.token, nil, maxMetadataBytes, &ref); err != nil {
		return "", err
	}
	if ref.Ref != "refs/heads/"+branch || ref.Object.Type != "commit" || !gitSHA.MatchString(ref.Object.SHA) {
		return "", ErrUnavailable
	}
	return ref.Object.SHA, nil
}

// Publications exposes only the current actor's durable publication requests.
// Retained requests remain recoverable after a browser or daemon restart.
func (s *Service) Publications(ctx context.Context, actor, project, run string) ([]Publication, error) {
	if err := s.catalog.Authorize(ctx, actor, project, catalog.ManageProject); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,spec,pr_state,pr_number FROM central_repository_publications WHERE actor=? AND project=? AND json_extract(spec,'$.Request.run_id')=? ORDER BY CASE WHEN pr_state='done' THEN 1 ELSE 0 END,rowid DESC LIMIT 100`, actor, project, run)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Publication{}
	for rows.Next() {
		var id, state string
		var raw []byte
		var number int64
		if err = rows.Scan(&id, &raw, &state, &number); err != nil {
			return nil, err
		}
		var spec publishSpec
		if json.Unmarshal(raw, &spec) != nil {
			return nil, ErrUnavailable
		}
		if spec.Request.RunID != run {
			continue
		}
		item := Publication{ID: id, State: "pending", Branch: "kingdom/" + id}
		if state == "done" {
			item.State = "published"
			item.PullRequest = number
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
func (s *Service) Reconcile(ctx context.Context, actor, project, id string) (Publication, error) {
	if err := s.catalog.Authorize(ctx, actor, project, catalog.ManageProject); err != nil {
		return Publication{}, err
	}
	var key string
	var raw []byte
	if err := s.db.QueryRowContext(ctx, `SELECT request_key,spec FROM central_repository_publications WHERE actor=? AND project=? AND id=?`, actor, project, id).Scan(&key, &raw); err != nil {
		return Publication{}, ErrDenied
	}
	var spec publishSpec
	if json.Unmarshal(raw, &spec) != nil {
		return Publication{}, ErrUnavailable
	}
	return s.Publish(ctx, actor, project, key, spec.Request)
}
