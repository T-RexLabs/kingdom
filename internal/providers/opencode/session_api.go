package opencode

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"time"
)

// ModelRef selects a model and, optionally, one of its exact variant IDs.
type ModelRef struct {
	ProviderID string `json:"providerID"`
	ID         string `json:"id"`
	Variant    string `json:"variant,omitempty"`
}

// SessionSpec creates a session with a caller-chosen ID, so Kingdom can persist
// the identity before the server acknowledges it.
type SessionSpec struct {
	ID    string
	Title string
	Agent string
	Model ModelRef
}

// Agent is the effective profile definition that the server resolves for a
// location. Kingdom compares it with the profile it registered.
type Agent struct {
	ID          string       `json:"id"`
	System      string       `json:"system"`
	Mode        string       `json:"mode"`
	Hidden      bool         `json:"hidden"`
	Permissions []Permission `json:"permissions"`
}

type Permission struct {
	Action   string `json:"action"`
	Resource string `json:"resource"`
	Effect   string `json:"effect"`
}

// maxEventLine bounds one server-sent event so a hostile or broken stream
// cannot exhaust memory.
const maxEventLine = 16 << 20

func requireDirectory(directory string) error {
	if !filepath.IsAbs(directory) {
		return fmt.Errorf("OpenCode location must be an absolute directory")
	}
	return nil
}

func sessionPath(sessionID, suffix string) string {
	return "/api/session/" + url.PathEscape(sessionID) + suffix
}

func (c *Client) CreateSession(ctx context.Context, directory string, spec SessionSpec) error {
	if err := requireDirectory(directory); err != nil {
		return err
	}
	var created struct {
		Data struct {
			ID       string `json:"id"`
			Agent    string `json:"agent"`
			Location struct {
				Directory string `json:"directory"`
			} `json:"location"`
		} `json:"data"`
	}
	body := map[string]any{
		"id": spec.ID, "title": spec.Title, "agent": spec.Agent, "model": spec.Model,
		"location": map[string]string{"directory": directory}, "permissions": []any{},
	}
	if err := c.send(ctx, c.http, http.MethodPost, "/api/session", nil, body, &created); err != nil {
		return err
	}
	if created.Data.ID != spec.ID || created.Data.Agent != spec.Agent || filepath.Clean(created.Data.Location.Directory) != filepath.Clean(directory) {
		return &ProtocolError{Operation: "session creation", Cause: errors.New("created session differs from the request")}
	}
	return nil
}

func (c *Client) SwitchAgent(ctx context.Context, directory, sessionID, agent string) error {
	return c.send(ctx, c.http, http.MethodPost, sessionPath(sessionID, "/agent"), atLocation(directory), map[string]string{"agent": agent}, nil)
}

// PendingInbox counts durable session work that has not been delivered.
func (c *Client) PendingInbox(ctx context.Context, directory, sessionID string) (int, error) {
	var inbox struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := c.send(ctx, c.http, http.MethodGet, sessionPath(sessionID, "/inbox"), atLocation(directory), nil, &inbox); err != nil {
		return 0, err
	}
	return len(inbox.Data), nil
}

// Prompt admits one user input with a caller-chosen message ID. Steer delivery
// keeps it from queuing behind unrelated work.
func (c *Client) Prompt(ctx context.Context, directory, sessionID, messageID, text string) error {
	var admitted struct {
		Data struct {
			ID   string `json:"id"`
			Type string `json:"type"`
		} `json:"data"`
	}
	body := map[string]string{"id": messageID, "text": text, "delivery": "steer"}
	if err := c.send(ctx, c.http, http.MethodPost, sessionPath(sessionID, "/prompt"), atLocation(directory), body, &admitted); err != nil {
		return err
	}
	if admitted.Data.ID != messageID || admitted.Data.Type != "user" {
		return &ProtocolError{Operation: "prompt admission", Cause: errors.New("admitted input differs from the request")}
	}
	return nil
}

// Wait returns when the session agent loop is idle. It does not distinguish
// success from interruption; callers verify the terminal evidence.
func (c *Client) Wait(ctx context.Context, directory, sessionID string) error {
	return c.send(ctx, c.long, http.MethodPost, "/api/experimental/session/"+url.PathEscape(sessionID)+"/wait", atLocation(directory), map[string]any{}, nil)
}

// Interrupt stops only this session's active execution and parks its queue.
func (c *Client) Interrupt(ctx context.Context, directory, sessionID string) error {
	query := atLocation(directory)
	query.Set("resume", "false")
	var interrupted struct {
		Interrupted bool `json:"interrupted"`
	}
	return c.send(ctx, c.http, http.MethodPost, sessionPath(sessionID, "/interrupt"), query, map[string]any{}, &interrupted)
}

// SessionInfo is the agent and model that the server will use for a session's
// next step.
type SessionInfo struct {
	ID       string   `json:"id"`
	Agent    string   `json:"agent"`
	Model    ModelRef `json:"model"`
	Location struct {
		Directory string `json:"directory"`
	} `json:"location"`
}

func (c *Client) Session(ctx context.Context, directory, sessionID string) (SessionInfo, error) {
	var found struct {
		Data SessionInfo `json:"data"`
	}
	if err := c.send(ctx, c.http, http.MethodGet, sessionPath(sessionID, ""), atLocation(directory), nil, &found); err != nil {
		return SessionInfo{}, err
	}
	if found.Data.ID != sessionID {
		return SessionInfo{}, &ProtocolError{Operation: "session lookup", Cause: errors.New("session identity differs from the request")}
	}
	return found.Data, nil
}

func (c *Client) SwitchModel(ctx context.Context, directory, sessionID string, model ModelRef) error {
	return c.send(ctx, c.http, http.MethodPost, sessionPath(sessionID, "/model"), atLocation(directory), map[string]ModelRef{"model": model}, nil)
}

// CallPlugin invokes a method that a plugin registered as public RPC at the
// location. Plugin state is per location spelling, so directory must be the
// canonical path used for every other call.
func (c *Client) CallPlugin(ctx context.Context, directory, plugin, method string, input, output any) error {
	var result struct {
		Output json.RawMessage `json:"output"`
	}
	if err := c.send(ctx, c.http, http.MethodPost, "/api/rpc/"+url.PathEscape(plugin)+"/"+url.PathEscape(method), atLocation(directory), map[string]any{"input": input}, &result); err != nil {
		return err
	}
	if err := json.Unmarshal(result.Output, output); err != nil {
		return &ProtocolError{Operation: "plugin " + plugin + "." + method, Cause: err}
	}
	return nil
}

func (c *Client) Agent(ctx context.Context, directory, agentID string) (Agent, error) {
	var found struct {
		Data Agent `json:"data"`
	}
	if err := c.send(ctx, c.http, http.MethodGet, "/api/agent/"+url.PathEscape(agentID), atLocation(directory), nil, &found); err != nil {
		return Agent{}, err
	}
	if found.Data.ID != agentID {
		return Agent{}, &ProtocolError{Operation: "agent lookup", Cause: errors.New("agent identity differs from the request")}
	}
	return found.Data, nil
}

// messagePageSize bounds how many projected messages one response holds.
const messagePageSize = 25

// errStopVisit ends a Messages walk early without reporting a failure.
var errStopVisit = errors.New("stop visiting")

// Messages visits the session's projected messages newest first, which is how
// the server pages them. Each visited message is one raw JSON value owned by
// the callback. Returning errStopVisit ends the walk successfully.
func (c *Client) Messages(ctx context.Context, directory, sessionID string, visit func(json.RawMessage) error) error {
	cursor := ""
	for {
		query := atLocation(directory)
		query.Set("limit", strconv.Itoa(messagePageSize))
		if cursor == "" {
			query.Set("order", "desc")
		} else {
			// The opaque cursor carries ordering; OpenCode rejects order when
			// it accompanies a cursor, even if the value is unchanged.
			query.Set("cursor", cursor)
		}
		var page struct {
			Data   []json.RawMessage `json:"data"`
			Cursor struct {
				Next string `json:"next"`
			} `json:"cursor"`
		}
		if err := c.send(ctx, c.http, http.MethodGet, sessionPath(sessionID, "/message"), query, nil, &page); err != nil {
			return err
		}
		for _, message := range page.Data {
			if err := visit(message); err != nil {
				if errors.Is(err, errStopVisit) {
					return nil
				}
				return err
			}
		}
		if page.Cursor.Next == "" {
			return nil
		}
		cursor = page.Cursor.Next
	}
}

// Log reads the finite public event log of one session up to its sync marker
// and returns the marker's aggregate sequence. It supplies a watermark; it is
// not a recovery source because the server keeps no historical event payloads.
func (c *Client) Log(ctx context.Context, directory, sessionID string, visit func(json.RawMessage) error) (int64, error) {
	query := atLocation(directory)
	query.Set("follow", "false")
	response, err := c.open(ctx, c.long, http.MethodGet, "/api/experimental/session/"+url.PathEscape(sessionID)+"/log", query, nil)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, &ServiceError{Operation: "session log", StatusCode: response.StatusCode}
	}
	watermark := int64(-1)
	err = readEventStream(response.Body, func(data []byte) error {
		if watermark >= 0 {
			return &ProtocolError{Operation: "session log", Cause: errors.New("event follows the sync marker")}
		}
		var marker struct {
			Type string `json:"type"`
			Seq  int64  `json:"seq"`
		}
		if err := json.Unmarshal(data, &marker); err != nil {
			return &ProtocolError{Operation: "session log", Cause: err}
		}
		if marker.Type == "log.synced" {
			watermark = marker.Seq
			return nil
		}
		return visit(json.RawMessage(data))
	})
	if err != nil {
		return 0, err
	}
	if watermark < 0 {
		return 0, &ProtocolError{Operation: "session log", Cause: errors.New("log ended without a sync marker")}
	}
	return watermark, nil
}

// FollowEvents streams every public service event until the context ends or
// the stream breaks. A returned error, including a clean EOF, means the stream
// is no longer continuous evidence.
func (c *Client) FollowEvents(ctx context.Context, connected func(), visit func(json.RawMessage) error) error {
	response, err := c.open(ctx, c.long, http.MethodGet, "/api/event", nil, nil)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return &ServiceError{Operation: "/api/event", StatusCode: response.StatusCode}
	}
	first := true
	err = readEventStream(response.Body, func(data []byte) error {
		if first {
			first = false
			var hello struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(data, &hello); err != nil || hello.Type != "server.connected" {
				return &ProtocolError{Operation: "/api/event", Cause: errors.New("stream did not begin with a connection event")}
			}
			connected()
			return nil
		}
		return visit(json.RawMessage(data))
	})
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	return err
}

// readEventStream delivers each server-sent event's data payload. Payload
// slices are only valid during the callback.
func readEventStream(body io.Reader, visit func([]byte) error) error {
	reader := bufio.NewReaderSize(body, 64<<10)
	var data []byte
	for {
		line, err := readBoundedLine(reader)
		if err != nil {
			if errors.Is(err, io.EOF) && len(data) == 0 {
				return nil
			}
			return err
		}
		switch {
		case len(line) == 0:
			if len(data) > 0 {
				if err := visit(data); err != nil {
					return err
				}
				data = data[:0]
			}
		case bytes.HasPrefix(line, []byte("data:")):
			if len(data) > 0 {
				data = append(data, '\n')
			}
			data = append(data, bytes.TrimPrefix(bytes.TrimPrefix(line, []byte("data:")), []byte(" "))...)
		}
	}
}

func readBoundedLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		fragment, err := reader.ReadSlice('\n')
		line = append(line, fragment...)
		if len(line) > maxEventLine {
			return nil, &ProtocolError{Operation: "event stream", Cause: errors.New("event exceeds the size limit")}
		}
		if err == nil {
			return bytes.TrimRight(line, "\r\n"), nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			if errors.Is(err, io.EOF) && len(line) > 0 {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
}

// retryTransient repeats an idempotent call after a transport failure or a
// server error, with a short delay, until it succeeds, fails definitively, or
// the context ends.
func retryTransient(ctx context.Context, call func() error) error {
	delay := 100 * time.Millisecond
	for {
		err := call()
		if err == nil || !isTransient(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(delay):
		}
		delay = min(delay*2, 2*time.Second)
	}
}

func isTransient(err error) bool {
	var service *ServiceError
	if errors.As(err, &service) {
		return service.StatusCode >= http.StatusInternalServerError
	}
	var protocol *ProtocolError
	return !errors.As(err, &protocol)
}

// PluginState is a loaded plugin's status at a location.
type PluginState struct {
	ID    string `json:"id"`
	State struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	} `json:"state"`
}

// Plugins lists the plugins that the server has loaded, or is loading, at a
// location. Loading is lazy, so a plugin can be absent just after a request.
func (c *Client) Plugins(ctx context.Context, directory string) ([]PluginState, error) {
	var plugins struct {
		Data []PluginState `json:"data"`
	}
	if err := c.send(ctx, c.http, http.MethodGet, "/api/plugin", atLocation(directory), nil, &plugins); err != nil {
		return nil, err
	}
	return plugins.Data, nil
}
