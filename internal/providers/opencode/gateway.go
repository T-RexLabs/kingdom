package opencode

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
)

const gatewayBodyLimit = 8 << 20

// nativeAdmission is the plugin's request to run one stock native tool.
type nativeAdmission struct {
	Identity  providerexecution.RequestIdentity `json:"identity"`
	MessageID string                            `json:"messageID"`
	CallID    string                            `json:"callID"`
	Tool      string                            `json:"tool"`
	Input     json.RawMessage                   `json:"input"`
}

// nativeReceipt reports how an admitted native tool ended. It is also the
// durable outbox record, so recovery can deliver it after a failed attempt.
type nativeReceipt struct {
	Identity       providerexecution.RequestIdentity `json:"identity"`
	GrantID        string                            `json:"grantID"`
	IdentityDigest string                            `json:"identityDigest"`
	Outcome        string                            `json:"outcome"`
	NativeStarted  *bool                             `json:"nativeStarted"`
}

func (r nativeReceipt) providerReceipt() providerrequest.NativeReceipt {
	return providerrequest.NativeReceipt{GrantID: r.GrantID, Digest: r.IdentityDigest, Outcome: r.Outcome, Started: r.NativeStarted}
}

type commandCancellation struct {
	Identity providerexecution.RequestIdentity `json:"identity"`
}

// gateway is the loopback ingress through which the Kingdom plugin asks for
// admissions and reports receipts. The bearer token authenticates the plugin;
// the identity inside each call selects the durable binding that decides it.
type gateway struct {
	authority *providerrequest.Authority
	commands  *commandBridge
	token     string
	listener  net.Listener
	server    *http.Server
}

func newGateway(authority *providerrequest.Authority, commands *commandBridge) (*gateway, error) {
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, fmt.Errorf("generate gateway credential: %w", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen for the OpenCode plugin: %w", err)
	}
	g := &gateway{authority: authority, commands: commands, token: hex.EncodeToString(secret[:]), listener: listener}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/native/admit", g.handle(g.admit))
	mux.HandleFunc("/v1/native/finish", g.handle(g.finish))
	mux.HandleFunc("/v1/command/invoke", g.handle(g.invoke))
	mux.HandleFunc("/v1/command/cancel", g.handle(g.cancel))
	g.server = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = g.server.Serve(listener) }()
	return g, nil
}

func (g *gateway) url() string { return "http://" + g.listener.Addr().String() }

func (g *gateway) close(ctx context.Context) error {
	return g.server.Shutdown(ctx)
}

func (g *gateway) authorized(r *http.Request) bool {
	presented := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return subtle.ConstantTimeCompare([]byte(presented), []byte(g.token)) == 1
}

// handle decodes one strict JSON request and reports every refusal as a
// conflict, which the plugin turns into a failed tool call.
func (g *gateway) handle(route func(context.Context, json.RawMessage) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost || !g.authorized(r) {
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "forbidden"})
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, gatewayBodyLimit))
		var response any
		if err == nil {
			response, err = route(r.Context(), raw)
		}
		if err != nil {
			w.WriteHeader(http.StatusConflict)
			response = map[string]string{"error": err.Error()}
		}
		_ = json.NewEncoder(w).Encode(response)
	}
}

func decodeStrict(raw json.RawMessage, destination any) error {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("request must contain one JSON value")
	}
	return nil
}

func (g *gateway) admit(ctx context.Context, raw json.RawMessage) (any, error) {
	var in nativeAdmission
	if err := decodeStrict(raw, &in); err != nil {
		return nil, err
	}
	grant, err := g.authority.AdmitNative(ctx, providerrequest.NativeCall{Identity: in.Identity, MessageID: in.MessageID, CallID: in.CallID, Tool: in.Tool, Input: in.Input})
	if err != nil {
		return nil, err
	}
	return map[string]any{"execute": grant.Execute, "grantID": grant.ID, "identityDigest": grant.Digest}, nil
}

func (g *gateway) finish(ctx context.Context, raw json.RawMessage) (any, error) {
	var receipt nativeReceipt
	if err := decodeStrict(raw, &receipt); err != nil {
		return nil, err
	}
	if err := g.authority.FinishNative(ctx, receipt.Identity, receipt.providerReceipt()); err != nil {
		return nil, err
	}
	return map[string]bool{"acknowledged": true}, nil
}

func (g *gateway) invoke(ctx context.Context, raw json.RawMessage) (any, error) {
	var in commandInput
	if err := decodeStrict(raw, &in); err != nil {
		return nil, err
	}
	return g.commands.invoke(ctx, in)
}

func (g *gateway) cancel(ctx context.Context, raw json.RawMessage) (any, error) {
	var in commandCancellation
	if err := decodeStrict(raw, &in); err != nil {
		return nil, err
	}
	if err := g.commands.cancel(ctx, in.Identity); err != nil {
		return nil, err
	}
	return map[string]bool{"closed": true}, nil
}
