package api

import (
	"context"
	"errors"
	"testing"
)

type countedPlannerCloser struct{ calls int }

func (c *countedPlannerCloser) Close() error {
	c.calls++
	if c.calls == 1 {
		return errors.New("transient scratch cleanup failure")
	}
	return nil
}

func TestServerOwnsPlannerAdapterCleanupAndRetriesFailure(t *testing.T) {
	closer := &countedPlannerCloser{}
	server := &Server{plannerAdapterCloser: closer}
	if err := server.Close(context.Background()); err == nil {
		t.Fatal("planner adapter close failure was lost")
	}
	if err := server.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if closer.calls != 2 {
		t.Fatalf("close calls=%d", closer.calls)
	}
}
