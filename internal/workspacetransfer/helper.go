package workspacetransfer

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"kingdom/internal/securefs"
	"kingdom/internal/workspacearchive"
)

// Run is the production entry point. The image must deny writes to HelperPath
// and its ancestors, disable include_workdir, and allow only fixed workspace
// writes. All workload writers and their model capability must be closed/revoked
// before export. The helper neither receives nor resolves provider credentials.
func Run(ctx context.Context, job Job, stdout io.Writer) error {
	if err := job.Validate(); err != nil {
		return err
	}
	executable, err := os.Open("/proc/self/exe")
	if err != nil {
		return err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, executable)
	closeErr := executable.Close()
	if copyErr != nil || closeErr != nil {
		return errors.Join(copyErr, closeErr)
	}
	if hex.EncodeToString(hash.Sum(nil)) != job.HelperSHA256 {
		return errors.New("workspace helper identity mismatch")
	}
	return serve(ctx, job, WorkspaceParent, stdout, func() (net.Listener, error) { return net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", Port)) })
}

type transferResult struct {
	completion Completion
	err        error
}

func serve(parent context.Context, job Job, root string, stdout io.Writer, listen func() (net.Listener, error)) error {
	if err := job.Validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(job.TimeoutSeconds)*time.Second)
	defer cancel()
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("invalid workspace root")
	}
	scratch, err := os.MkdirTemp(root, ".kingdom-transfer-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	var exported workspacearchive.Export
	var archive *os.File
	if job.Mode == "export" {
		project, err := securefs.OpenRoot(filepath.Join(root, "project"))
		if err != nil {
			return err
		}
		defer project.Close()
		archive, err = os.OpenFile(filepath.Join(scratch, "archive.zip"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		defer archive.Close()
		exported, err = workspacearchive.ExportZIP(ctx, project, archive, job.Limits)
		if err != nil {
			return err
		}
		if err = archive.Sync(); err != nil {
			return err
		}
	}
	listener, err := listen()
	if err != nil {
		return err
	}
	listener = &boundedListener{Listener: listener, slots: make(chan struct{}, 4)}
	results := make(chan transferResult, 1)
	var claimed atomic.Bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "managed.invalid" || r.URL.RawQuery != "" || r.URL.IsAbs() || r.URL.RawPath != "" || len(r.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+job.Nonce)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		if r.Method == "GET" && r.URL.Path == "/v1/ready" {
			if claimed.Load() {
				http.Error(w, "transfer already claimed", http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		method := "GET"
		if job.Mode == "import" {
			method = "PUT"
		}
		if r.Method != method || r.URL.Path != "/v1/archive" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if !claimed.CompareAndSwap(false, true) {
			http.Error(w, "transfer already claimed", http.StatusConflict)
			return
		}
		result := transferResult{}
		defer func() { results <- result }()
		if job.Mode == "import" {
			if r.ContentLength != job.ArchiveBytes {
				result.err = errors.New("import size mismatch")
			} else {
				r.Body = http.MaxBytesReader(w, r.Body, job.ArchiveBytes)
				result.completion, result.err = receive(r.Context(), r.Body, root, scratch, job)
			}
			if result.err != nil {
				http.Error(w, "workspace import failed", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			result.err = http.NewResponseController(w).Flush()
			return
		}
		if r.ContentLength > 0 {
			result.err = errors.New("export request contains a body")
			http.Error(w, "unexpected body", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Length", fmt.Sprint(exported.ArchiveBytes))
		n, err := securefs.Copy(r.Context(), w, io.NewSectionReader(archive, 0, exported.ArchiveBytes))
		if err == nil && n != exported.ArchiveBytes {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			err = http.NewResponseController(w).Flush()
		}
		if err == nil {
			err = r.Context().Err()
		}
		result.err = err
		result.completion = completionFrom("export", exported.ArchiveBytes, exported.Manifest, exported.Digest)
	})
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: time.Duration(job.TimeoutSeconds) * time.Second, WriteTimeout: time.Duration(job.TimeoutSeconds) * time.Second, MaxHeaderBytes: 8 << 10, BaseContext: func(net.Listener) context.Context { return ctx }}
	server.SetKeepAlivesEnabled(false)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	var result transferResult
	select {
	case result = <-results:
	case <-ctx.Done():
		result.err = ctx.Err()
	case err := <-served:
		result.err = err
	}
	if result.err != nil {
		cancel()
		_ = server.Close()
		return result.err
	}
	if err := server.Shutdown(ctx); err != nil {
		_ = server.Close()
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(result.completion)
}
func completionFrom(mode string, size int64, manifest workspacearchive.Manifest, digest string) Completion {
	return Completion{Version: 1, Mode: mode, ArchiveSHA256: manifest.ArchiveSHA256, ArchiveBytes: size, ManifestSHA256: digest, ExpandedBytes: manifest.ExpandedBytes, Entries: len(manifest.Entries)}
}
func receive(ctx context.Context, input io.Reader, root, scratch string, job Job) (Completion, error) {
	file, err := os.OpenFile(filepath.Join(scratch, "input.zip"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return Completion{}, err
	}
	defer file.Close()
	hash := sha256.New()
	n, err := securefs.Copy(ctx, io.MultiWriter(file, hash), io.LimitReader(input, job.ArchiveBytes+1))
	if err != nil {
		return Completion{}, err
	}
	if n != job.ArchiveBytes || hex.EncodeToString(hash.Sum(nil)) != job.ArchiveSHA256 {
		return Completion{}, errors.New("import archive identity mismatch")
	}
	staged, err := workspacearchive.ImportZIP(ctx, scratch, file, n, job.Limits)
	if err != nil {
		return Completion{}, err
	}
	defer staged.Close()
	if err := ctx.Err(); err != nil {
		return Completion{}, err
	}
	destination := filepath.Join(root, "project")
	// Publish only into an absent directory; never replace an existing project.
	if err = publishDirectory(staged.Path, destination); err != nil {
		return Completion{}, err
	}
	parent, err := securefs.OpenRoot(root)
	if err != nil {
		return Completion{}, err
	}
	defer parent.Close()
	if err = parent.SyncDir("project"); err != nil {
		return Completion{}, err
	}
	if err = parent.SyncDir(""); err != nil {
		return Completion{}, err
	}
	if err := ctx.Err(); err != nil {
		return Completion{}, err
	}
	return completionFrom("import", n, staged.Manifest, staged.Digest), nil
}

// Bound concurrent HTTP handlers, including unauthenticated slow clients.
type boundedListener struct {
	net.Listener
	slots chan struct{}
}

func (l *boundedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &boundedConn{Conn: conn, release: func() { <-l.slots }}, nil
		default:
			_ = conn.Close()
		}
	}
}

type boundedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *boundedConn) Close() error { err := c.Conn.Close(); c.once.Do(c.release); return err }
