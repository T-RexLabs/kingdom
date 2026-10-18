package opencode

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// capture keeps the one public event subscription of a server process. It is
// opened before any managed session exists, so a tracked session's events are
// retained from its origin. Any end of the subscription permanently breaks
// continuity: stock servers keep no event history to repair it.
type capture struct {
	dir    string
	cancel context.CancelFunc
	done   chan struct{}

	mu       sync.Mutex
	err      error
	changed  chan struct{}
	sessions map[string]*capturedSession
}

type capturedSession struct {
	file      *os.File
	lastSeq   int64
	lastEvent time.Time
	spoke     bool
	feed      *eventFeed
}

// eventFeed delivers a session's live events, including non-durable ones such
// as text deltas, to one consumer. It never blocks the shared subscription.
type eventFeed struct {
	mu     sync.Mutex
	queue  []json.RawMessage
	wake   chan struct{}
	closed bool
}

func newEventFeed() *eventFeed { return &eventFeed{wake: make(chan struct{}, 1)} }

func (f *eventFeed) push(raw json.RawMessage) {
	f.mu.Lock()
	if !f.closed {
		f.queue = append(f.queue, append(json.RawMessage(nil), raw...))
	}
	f.mu.Unlock()
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// next blocks for the next event. It reports false when the feed is closed and
// drained or when ctx ends.
func (f *eventFeed) next(ctx context.Context) (json.RawMessage, bool) {
	for {
		f.mu.Lock()
		if len(f.queue) > 0 {
			raw := f.queue[0]
			f.queue = f.queue[1:]
			f.mu.Unlock()
			return raw, true
		}
		closed := f.closed
		f.mu.Unlock()
		if closed {
			return nil, false
		}
		select {
		case <-f.wake:
		case <-ctx.Done():
			return nil, false
		}
	}
}

func (f *eventFeed) close() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func startCapture(ctx context.Context, client *Client, dir string) (*capture, error) {
	runCtx, cancel := context.WithCancel(context.Background())
	c := &capture{dir: dir, cancel: cancel, done: make(chan struct{}), changed: make(chan struct{}), sessions: map[string]*capturedSession{}}
	connected := make(chan struct{})
	go func() {
		defer close(c.done)
		c.fail(client.FollowEvents(runCtx, func() { close(connected) }, c.record))
	}()
	select {
	case <-connected:
		return c, nil
	case <-c.done:
		return nil, c.healthy()
	case <-ctx.Done():
		c.stop()
		return nil, ctx.Err()
	}
}

// stop ends the subscription and releases the spool files.
func (c *capture) stop() {
	c.cancel()
	<-c.done
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, session := range c.sessions {
		_ = session.file.Close()
		if session.feed != nil {
			session.feed.close()
		}
	}
	c.sessions = map[string]*capturedSession{}
}

func (c *capture) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = fmt.Errorf("OpenCode event capture ended: %w", err)
	}
	c.broadcast()
}

// broadcast wakes waiters; the caller holds the mutex.
func (c *capture) broadcast() {
	close(c.changed)
	c.changed = make(chan struct{})
}

// healthy reports whether every event since the subscription began was kept.
func (c *capture) healthy() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// track starts retaining a session's durable events. It must precede the
// session's creation so that the origin event is retained.
func (c *capture) track(sessionID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	if _, ok := c.sessions[sessionID]; ok {
		return nil
	}
	file, err := os.OpenFile(c.path(sessionID), os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("track OpenCode session events: %w", err)
	}
	c.sessions[sessionID] = &capturedSession{file: file, lastSeq: -1, lastEvent: time.Now()}
	return nil
}

func (c *capture) path(sessionID string) string { return filepath.Join(c.dir, sessionID+".events") }

func (c *capture) record(raw json.RawMessage) error {
	var event struct {
		Durable *struct {
			AggregateID string `json:"aggregateID"`
			Seq         *int64 `json:"seq"`
		} `json:"durable"`
		Data struct {
			SessionID string `json:"sessionID"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return fmt.Errorf("decode OpenCode event: %w", err)
	}
	sessionID := event.Data.SessionID
	if sessionID == "" && event.Durable != nil {
		sessionID = event.Durable.AggregateID
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	session, ok := c.sessions[sessionID]
	if !ok {
		return nil
	}
	session.lastEvent, session.spoke = time.Now(), true
	if session.feed != nil {
		session.feed.push(raw)
	}
	if event.Durable == nil || event.Durable.Seq == nil {
		return nil
	}
	if *event.Durable.Seq <= session.lastSeq {
		return fmt.Errorf("OpenCode session %s event order is not increasing", sessionID)
	}
	var line bytes.Buffer
	if err := json.Compact(&line, raw); err != nil {
		return err
	}
	line.WriteByte('\n')
	if _, err := session.file.Write(line.Bytes()); err != nil {
		return fmt.Errorf("retain OpenCode event: %w", err)
	}
	session.lastSeq = *event.Durable.Seq
	c.broadcast()
	return nil
}

// tracked reports whether this subscription has retained the session since its
// origin, which is what makes its history usable as evidence.
func (c *capture) tracked(sessionID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.sessions[sessionID]
	return ok
}

// follow starts a feed of the session's live events.
func (c *capture) follow(sessionID string) (*eventFeed, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, ok := c.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("OpenCode session %s is not tracked", sessionID)
	}
	if session.feed != nil {
		session.feed.close()
	}
	session.feed = newEventFeed()
	session.lastEvent, session.spoke = time.Now(), false
	return session.feed, nil
}

// quiet reports how long the session has produced no event, and whether it has
// produced any since its feed began.
func (c *capture) quiet(sessionID string) (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if session, ok := c.sessions[sessionID]; ok {
		return time.Since(session.lastEvent), session.spoke
	}
	return 0, false
}

// await blocks until the session's events through watermark were retained.
func (c *capture) await(ctx context.Context, sessionID string, watermark int64) error {
	for {
		c.mu.Lock()
		session, ok := c.sessions[sessionID]
		err, changed := c.err, c.changed
		reached := ok && session.lastSeq >= watermark
		c.mu.Unlock()
		switch {
		case !ok:
			return fmt.Errorf("OpenCode session %s is not tracked", sessionID)
		case reached:
			return nil
		case err != nil:
			return err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// writeInterval writes a JSON array of the session's retained events from the
// delivery of requestID through the watermark. Events after the watermark were
// not part of the verified boundary and are omitted.
func (c *capture) writeInterval(sessionID, requestID string, watermark int64, out io.Writer) error {
	if err := c.healthy(); err != nil {
		return err
	}
	file, err := os.Open(c.path(sessionID))
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 64<<10)
	if _, err := io.WriteString(out, "["); err != nil {
		return err
	}
	delivered := false
	for count := 0; ; {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) && len(line) == 0 {
			break
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		var event struct {
			Type    string `json:"type"`
			Durable struct {
				Seq int64 `json:"seq"`
			} `json:"durable"`
			Data struct {
				InboxID string `json:"inboxID"`
			} `json:"data"`
		}
		if err := json.Unmarshal(line, &event); err != nil {
			return err
		}
		if event.Durable.Seq > watermark {
			break
		}
		delivered = delivered || (event.Type == "session.inbox.delivered" && event.Data.InboxID == requestID)
		if !delivered {
			continue
		}
		if count > 0 {
			if _, err := io.WriteString(out, ","); err != nil {
				return err
			}
		}
		if _, err := out.Write(bytes.TrimRight(line, "\n")); err != nil {
			return err
		}
		count++
	}
	if !delivered {
		return fmt.Errorf("OpenCode request %s was not delivered in the retained events", requestID)
	}
	_, err = io.WriteString(out, "]")
	return err
}
