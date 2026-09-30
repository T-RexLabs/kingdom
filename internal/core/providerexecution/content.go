package providerexecution

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"kingdom/internal/securefs"
)

// ContentReference identifies immutable bytes within one provider invocation.
// It is evidence transport, never completion authority.
type ContentReference struct {
	InvocationID string `json:"invocation_id"`
	SHA256       string `json:"sha256"`
	SizeBytes    int64  `json:"size_bytes"`
}

// StagedContent is process-local transport to the runtime's trusted staging
// store. Paths are never replay authority and are excluded from durable JSON.
type StagedContent struct {
	Reference ContentReference `json:"reference"`
	Path      string           `json:"-"`
	Offset    int64            `json:"-"`
}

type ContentOpener func(context.Context) (io.ReadCloser, error)

// Open pins a no-follow regular file and verifies the referenced byte range at
// EOF. Callers must drain before accepting content; closing early is not proof.
func (s StagedContent) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := s.Reference.Validate(s.Reference.InvocationID); err != nil {
		return nil, err
	}
	root, err := securefs.OpenRoot(filepath.Dir(s.Path))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	f, err := root.OpenRegular(filepath.Base(s.Path))
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if s.Offset < 0 || s.Offset > info.Size() || s.Reference.SizeBytes > info.Size()-s.Offset {
		f.Close()
		return nil, fmt.Errorf("provider content range is invalid")
	}
	return VerifyReader(ctx, s.Reference, &sectionReadCloser{Reader: io.NewSectionReader(f, s.Offset, s.Reference.SizeBytes), Closer: f}), nil
}

type sectionReadCloser struct {
	io.Reader
	io.Closer
}

type verifiedReader struct {
	ctx     context.Context
	ref     ContentReference
	source  io.ReadCloser
	hash    hash.Hash
	size    int64
	done    bool
	failure error
}

func VerifyReader(ctx context.Context, ref ContentReference, source io.ReadCloser) io.ReadCloser {
	return &verifiedReader{ctx: ctx, ref: ref, source: source, hash: sha256.New()}
}
func (r *verifiedReader) Read(raw []byte) (int, error) {
	if r.failure != nil {
		return 0, r.failure
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.source.Read(raw)
	_, _ = r.hash.Write(raw[:n])
	r.size += int64(n)
	if r.size > r.ref.SizeBytes {
		r.failure = fmt.Errorf("provider content exceeds declared size")
		return n, r.failure
	}
	if err == io.EOF && !r.done {
		r.done = true
		if r.size != r.ref.SizeBytes || hex.EncodeToString(r.hash.Sum(nil)) != r.ref.SHA256 {
			r.failure = fmt.Errorf("provider content hash or size mismatch")
			return n, r.failure
		}
	}
	if err != nil && err != io.EOF {
		r.failure = err
	}
	return n, err
}
func (r *verifiedReader) Close() error { return r.source.Close() }

// TextSummary is bounded display data plus exact Unicode TrimSpace offsets.
// Offset/End refer to UTF-8 bytes in the original stream.
type TextSummary struct {
	Offset, End int64
	Preview     string
}

func InspectText(ctx context.Context, source io.Reader) (TextSummary, error) {
	r := bufio.NewReaderSize(source, ContentChunkBytes)
	result := TextSummary{Offset: -1}
	var position int64
	var preview strings.Builder
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		ch, size, err := r.ReadRune()
		if err == io.EOF {
			break
		}
		if err != nil {
			return result, err
		}
		if ch == utf8.RuneError && size == 1 {
			return result, fmt.Errorf("provider terminal text is not valid UTF-8")
		}
		if !unicode.IsSpace(ch) {
			if result.Offset < 0 {
				result.Offset = position
			}
			result.End = position + int64(size)
		}
		if result.Offset >= 0 && preview.Len() < 32*1024 {
			preview.WriteRune(ch)
		}
		position += int64(size)
	}
	if result.Offset < 0 {
		result.Offset = 0
		return result, nil
	}
	result.Preview = preview.String()
	if result.End-result.Offset <= int64(len(result.Preview)) {
		result.Preview = strings.TrimSpace(result.Preview)
	}
	return result, nil
}

func OpenTerminalText(ctx context.Context, o InvocationStreamObservation) (io.ReadCloser, error) {
	if o.CanonicalTerminalTextRef == nil {
		return io.NopCloser(strings.NewReader(o.CanonicalTerminalText)), nil
	}
	ref := *o.CanonicalTerminalTextRef
	if err := ref.Validate(ref.InvocationID); err != nil {
		return nil, err
	}
	if o.CanonicalTerminalText != "" && ReferenceText(ref.InvocationID, o.CanonicalTerminalText) != ref {
		return nil, fmt.Errorf("inline terminal text conflicts with its content reference")
	}
	if o.TerminalTextOpener == nil {
		return nil, fmt.Errorf("verified terminal content reader is required")
	}
	reader, err := o.TerminalTextOpener(ctx)
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, fmt.Errorf("terminal content reader is unavailable")
	}
	return VerifyReader(ctx, ref, reader), nil
}

func CopyTerminalText(ctx context.Context, o InvocationStreamObservation, dst io.Writer) error {
	r, err := OpenTerminalText(ctx, o)
	if err != nil {
		return err
	}
	_, copyErr := io.CopyBuffer(dst, struct{ io.Reader }{r}, make([]byte, ContentChunkBytes))
	return errors.Join(copyErr, r.Close())
}

// OpenContentCopy adapts a streaming store writer to a replay reader. Close
// cancels and joins its producer, including early-abandoned reads.
func OpenContentCopy(ctx context.Context, copyContent func(io.Writer) error) io.ReadCloser {
	ctx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := copyContent(contentContextWriter{ctx, writer})
		_ = writer.CloseWithError(err)
	}()
	return &copiedContentReader{PipeReader: reader, cancel: cancel, done: done}
}

type contentContextWriter struct {
	ctx context.Context
	io.Writer
}

func (w contentContextWriter) Write(raw []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.Writer.Write(raw)
}

type copiedContentReader struct {
	*io.PipeReader
	cancel context.CancelFunc
	done   <-chan struct{}
}

func (r *copiedContentReader) Close() error {
	r.cancel()
	err := r.PipeReader.Close()
	<-r.done
	return err
}

const ContentChunkBytes = 64 * 1024

func ReferenceText(invocationID, text string) ContentReference {
	h := sha256.New()
	_, _ = io.WriteString(h, text)
	return ContentReference{InvocationID: invocationID, SHA256: hex.EncodeToString(h.Sum(nil)), SizeBytes: int64(len(text))}
}

func (r ContentReference) Validate(invocationID string) error {
	if r.InvocationID == "" || r.InvocationID != invocationID || strings.TrimSpace(r.InvocationID) != r.InvocationID || r.SizeBytes < 0 {
		return fmt.Errorf("provider content lineage or size is invalid")
	}
	raw, err := hex.DecodeString(r.SHA256)
	if err != nil || len(raw) != sha256.Size || hex.EncodeToString(raw) != r.SHA256 {
		return fmt.Errorf("provider content digest is invalid")
	}
	return nil
}

func CompactObservation(o InvocationStreamObservation) InvocationStreamObservation {
	if o.CanonicalTerminalTextRef != nil {
		o.CanonicalTerminalText = ""
	}
	o.StagedTerminalContent = nil
	o.StagedDiagnosticContent = nil
	o.TerminalTextOpener = nil
	o.ReleaseContent = nil
	return o
}
