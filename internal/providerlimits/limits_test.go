package providerlimits

import (
	"errors"
	"io"
	"testing"
	"time"
)

func TestReadBoundedOnLimitReturnsWithoutWaitingForWriterClose(t *testing.T) {
	const maxBytes = 8

	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})

	type readResult struct {
		raw       []byte
		err       error
		cancelled bool
	}
	readDone := make(chan readResult, 1)
	go func() {
		cancelled := false
		raw, err := ReadBoundedOnLimit(reader, maxBytes, "test output", func() {
			cancelled = true
		})
		readDone <- readResult{raw: raw, err: err, cancelled: cancelled}
	}()

	writerDone := make(chan error, 1)
	go func() {
		_, err := writer.Write([]byte("123456789"))
		writerDone <- err
	}()
	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatalf("write over-limit payload: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer did not deliver maxBytes+1 bytes")
	}

	var result readResult
	select {
	case result = <-readDone:
	case <-time.After(time.Second):
		// Unblock the drain-to-EOF implementation so the failed test does not
		// leave its reader goroutine waiting on this still-open pipe.
		_ = writer.Close()
		select {
		case <-readDone:
		case <-time.After(time.Second):
			t.Fatal("bounded read remained blocked after closing the writer")
		}
		t.Fatal("bounded read waited for the writer to close after overflow")
	}

	if !result.cancelled {
		t.Fatal("over-limit read did not request cancellation")
	}
	var limitErr *LimitExceededError
	if !errors.As(result.err, &limitErr) || limitErr.Resource != "test output" || limitErr.Limit != maxBytes {
		t.Fatalf("over-limit read error=%v, want test output limit error", result.err)
	}
	if len(result.raw) > maxBytes {
		t.Fatalf("retained %d bytes, want no more than %d", len(result.raw), maxBytes)
	}
	if string(result.raw) != "12345678" {
		t.Fatalf("retained prefix=%q, want %q", result.raw, "12345678")
	}
}
