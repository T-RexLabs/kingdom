package cli

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

func TestSummonWarmCancellationJoinsWorker(t *testing.T) {
	started := make(chan struct{})
	result, stop := startSummonWarm(context.Background(), func(ctx context.Context) { close(started); <-ctx.Done() })
	<-started
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-result:
	default:
		t.Fatal("worker did not finish before stop returned")
	}
}
func TestSummonWarmNoncooperativeWorkerHasBoundedJoin(t *testing.T) {
	release := make(chan struct{})
	result, stop := startSummonWarm(context.Background(), func(context.Context) { <-release })
	defer close(release)
	started := time.Now()
	if err := stop(); err == nil {
		t.Fatal("unjoined worker reported stopped")
	}
	if time.Since(started) > 3*time.Second {
		t.Fatal("warm shutdown exceeded bound")
	}
	select {
	case <-result:
		t.Fatal("noncooperative worker unexpectedly finished")
	default:
	}
}
func TestSummonOutputSerializesConcurrentOwners(t *testing.T) {
	var b bytes.Buffer
	writer := &summonWriter{out: &b}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if _, err := writer.Write([]byte("entry\n")); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if b.Len() != 8*100*len("entry\n") {
		t.Fatalf("output lost or interleaved: %d", b.Len())
	}
}
