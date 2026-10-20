package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// eventServer streams the events a test sends, after the connection event.
func eventServer(t *testing.T) (*Client, chan string, func()) {
	t.Helper()
	events := make(chan string)
	stop := make(chan struct{})
	client := sessionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		io.WriteString(w, "data: {\"type\":\"server.connected\"}\n\n")
		flusher.Flush()
		for {
			select {
			case event := <-events:
				fmt.Fprintf(w, "data: %s\n\n", event)
				flusher.Flush()
			case <-stop:
				return
			case <-r.Context().Done():
				return
			}
		}
	})
	return client, events, func() { close(stop) }
}

func durableEvent(session string, seq int, eventType string, data string) string {
	return fmt.Sprintf(`{"id":"evt_%d","type":%q,"data":{"sessionID":%q%s},"durable":{"aggregateID":%q,"seq":%d}}`, seq, eventType, session, data, session, seq)
}

func startTestCapture(t *testing.T) (*capture, chan string, func()) {
	t.Helper()
	client, events, endStream := eventServer(t)
	c, err := startCapture(context.Background(), client, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.stop)
	return c, events, endStream
}

func TestCaptureRetainsOnlyTrackedSessionsDurableEventsFromTheirOrigin(t *testing.T) {
	c, events, _ := startTestCapture(t)
	if err := c.track("ses_a"); err != nil {
		t.Fatal(err)
	}
	for _, event := range []string{
		durableEvent("ses_a", 0, "session.created", ""),
		durableEvent("ses_other", 0, "session.created", ""),
		`{"id":"evt_delta","type":"session.text.delta","data":{"sessionID":"ses_a","delta":"x"}}`,
		durableEvent("ses_a", 1, "session.inbox.delivered", `,"inboxID":"msg_other"`),
		durableEvent("ses_a", 4, "session.inbox.delivered", `,"inboxID":"msg_req"`),
		durableEvent("ses_a", 5, "session.step.started", ""),
		durableEvent("ses_a", 9, "session.execution.succeeded", ""),
		durableEvent("ses_a", 10, "session.usage.settled", ""),
	} {
		events <- event
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.await(ctx, "ses_a", 10); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := c.writeInterval("ses_a", "msg_req", 9, &out); err != nil {
		t.Fatal(err)
	}
	var interval []struct {
		Type    string `json:"type"`
		Durable struct{ Seq int }
	}
	if err := json.Unmarshal(out.Bytes(), &interval); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	var got []string
	for _, event := range interval {
		got = append(got, fmt.Sprintf("%s@%d", event.Type, event.Durable.Seq))
	}
	if strings.Join(got, ",") != "session.inbox.delivered@4,session.step.started@5,session.execution.succeeded@9" {
		t.Fatalf("interval %v", got)
	}
	if err := c.writeInterval("ses_a", "msg_missing", 9, io.Discard); err == nil {
		t.Fatal("an interval for a request that was never delivered was produced")
	}
	if c.tracked("ses_other") || !c.tracked("ses_a") {
		t.Fatal("tracking is not limited to explicit sessions")
	}
}

func TestCaptureBreaksPermanentlyWhenOrderIsViolated(t *testing.T) {
	c, events, _ := startTestCapture(t)
	if err := c.track("ses_a"); err != nil {
		t.Fatal(err)
	}
	events <- durableEvent("ses_a", 5, "session.step.started", "")
	events <- durableEvent("ses_a", 5, "session.step.ended", "")
	deadline := time.Now().Add(5 * time.Second)
	for c.healthy() == nil {
		if time.Now().After(deadline) {
			t.Fatal("a repeated sequence did not break capture")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := c.track("ses_b"); err == nil {
		t.Fatal("a broken capture accepted a new session")
	}
	if err := c.writeInterval("ses_a", "msg", 5, io.Discard); err == nil {
		t.Fatal("a broken capture still produced evidence")
	}
}

func TestCaptureBreaksWhenTheServerEndsTheStream(t *testing.T) {
	c, _, endStream := startTestCapture(t)
	if err := c.track("ses_a"); err != nil {
		t.Fatal(err)
	}
	endStream()
	deadline := time.Now().Add(5 * time.Second)
	for c.healthy() == nil {
		if time.Now().After(deadline) {
			t.Fatal("the end of the subscription did not break capture")
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.await(ctx, "ses_a", 3); err == nil {
		t.Fatal("waiting for events that can no longer arrive succeeded")
	}
}

func TestCaptureReportsSilenceSinceItsFeedBegan(t *testing.T) {
	c, events, _ := startTestCapture(t)
	if err := c.track("ses_a"); err != nil {
		t.Fatal(err)
	}
	feed, err := c.follow("ses_a")
	if err != nil {
		t.Fatal(err)
	}
	if _, spoke := c.quiet("ses_a"); spoke {
		t.Fatal("a session that has not spoken was reported as active")
	}
	events <- `{"id":"evt_delta","type":"session.text.delta","data":{"sessionID":"ses_a","delta":"x"}}`
	if raw, ok := feed.next(context.Background()); !ok || !strings.Contains(string(raw), "delta") {
		t.Fatalf("feed event %s %v", raw, ok)
	}
	if quiet, spoke := c.quiet("ses_a"); !spoke || quiet > time.Second {
		t.Fatalf("quiet=%v spoke=%v", quiet, spoke)
	}
}
