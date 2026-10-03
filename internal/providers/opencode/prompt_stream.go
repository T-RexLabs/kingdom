package opencode

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
)

type promptProgressWriter struct {
	ctx      context.Context
	writer   io.Writer
	progress func()
}

func (w *promptProgressWriter) Write(raw []byte) (int, error) {
	total := 0
	for len(raw) > 0 {
		if err := w.ctx.Err(); err != nil {
			return total, err
		}
		n, err := w.writer.Write(raw[:min(len(raw), 64*1024)])
		total += n
		raw = raw[n:]
		if n > 0 && w.progress != nil {
			w.progress()
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

// trailingPromptNewlines implements the prompt framing rule without retaining
// a suffix of arbitrary length. Only newline count is buffered.
type trailingPromptNewlines struct {
	dst     io.Writer
	pending int64
	wrote   bool
}

func (w *trailingPromptNewlines) Write(raw []byte) (int, error) {
	n := len(raw)
	end := n
	for end > 0 && raw[end-1] == '\n' {
		end--
	}
	if end > 0 {
		if w.pending > 0 {
			chunk := strings.Repeat("\n", 4096)
			for w.pending > 0 {
				size := min(w.pending, int64(len(chunk)))
				if _, err := io.WriteString(w.dst, chunk[:size]); err != nil {
					return 0, err
				}
				w.pending -= size
			}
		}
		if _, err := w.dst.Write(raw[:end]); err != nil {
			return 0, err
		}
		w.wrote = true
	}
	w.pending += int64(n - end)
	return n, nil
}

func writeRequestPrompt(ctx context.Context, req ExecuteRequest, invocationID string, dst io.Writer) error {
	writer := &trailingPromptNewlines{dst: dst}
	var err error
	switch {
	case req.OpenPrompt != nil:
		reader, openErr := req.OpenPrompt(ctx)
		if openErr != nil {
			if reader != nil {
				return errors.Join(openErr, reader.Close())
			}
			return openErr
		}
		if reader == nil {
			return errors.New("prompt reader is required")
		}
		var once sync.Once
		var closeErr error
		closeReader := func() { once.Do(func() { closeErr = reader.Close() }) }
		stop := context.AfterFunc(ctx, closeReader)
		_, err = io.CopyBuffer(writer, struct{ io.Reader }{reader}, make([]byte, 64*1024))
		stop()
		closeReader()
		err = errors.Join(err, closeErr)
	case req.WritePrompt != nil:
		err = req.WritePrompt(ctx, writer)
	default:
		_, err = io.Copy(writer, strings.NewReader(req.Prompt))
	}
	if err != nil {
		return err
	}
	if !writer.wrote && writer.pending == 0 {
		return errors.New("prompt source is empty")
	}
	if writer.wrote {
		if _, err := io.WriteString(dst, "\n\n"); err != nil {
			return err
		}
	}
	for _, part := range providerInvocationPromptParts("", req.AttemptID, invocationID) {
		if _, err := io.WriteString(dst, part); err != nil {
			return err
		}
	}
	return nil
}

// WriteInvocationPrompt uses the provider dispatch framing for a captured context
// and its immutable invocation identity. It owns and closes the opened source.
func WriteInvocationPrompt(ctx context.Context, open func(context.Context) (io.ReadCloser, error), attemptID, invocationID string, dst io.Writer) error {
	return writeRequestPrompt(ctx, ExecuteRequest{OpenPrompt: open, AttemptID: attemptID}, invocationID, dst)
}
