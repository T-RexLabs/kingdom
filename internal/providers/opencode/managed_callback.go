package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/filetoolhelper"
)

// ManagedCallbackScope owns the authenticated handler until Release. Its
// identity comes from the activated invocation capability, never the body.
type ManagedCallbackScope struct {
	Context  context.Context
	Identity providerexecution.RequestIdentity
	Release  func()
}
type ManagedCallbackAuthorizer interface {
	// Receipt permits historical effect receipts and command cancellation after
	// admission fencing. It never permits starting a new effect.
	Begin(context.Context, string, bool) (ManagedCallbackScope, error)
}
type ManagedNativeTools interface {
	InvokeNative(context.Context, providerrequest.NativeCall) (filetoolhelper.Result, error)
}
type ManagedCommandInput struct {
	Identity                        providerexecution.RequestIdentity
	MessageID, CallID, Command, Cwd string
	Timeout                         int64
	ToolInput                       json.RawMessage
}
type ManagedCommandResult struct {
	Output      string `json:"output"`
	Exit        int    `json:"exit"`
	ExecutionID string `json:"executionID"`
	Closed      bool   `json:"closed"`
	Truncated   bool   `json:"truncated"`
}
type ManagedCommands interface {
	Invoke(context.Context, ManagedCommandInput) (ManagedCommandResult, error)
	Cancel(context.Context, providerexecution.RequestIdentity) error
}

// NewManagedCallbackHandler creates no listener and has no local executor.
// The worker mounts it behind its authenticated HTTPS endpoint.
func NewManagedCallbackHandler(auth ManagedCallbackAuthorizer, native ManagedNativeTools, commands ManagedCommands) (http.Handler, error) {
	if nilManagedDependency(auth) || nilManagedDependency(native) || nilManagedDependency(commands) {
		return nil, errors.New("managed callback dependencies required")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		refuse := func(status int) {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `{"error":"Managed callback refused"}`)
		}
		receipt := false
		switch r.URL.Path {
		case "/v1/native/invoke", "/v1/command/invoke":
		case "/v1/command/cancel":
			receipt = true
		default:
			refuse(http.StatusNotFound)
			return
		}
		values := r.Header.Values("Authorization")
		if r.Method != http.MethodPost || r.URL.RawQuery != "" || r.URL.RawPath != "" || len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
			refuse(http.StatusForbidden)
			return
		}
		token := strings.TrimPrefix(values[0], "Bearer ")
		if token == "" || len(token) > 256 || strings.ContainsAny(token, " \t\r\n") {
			refuse(http.StatusForbidden)
			return
		}
		scope, err := auth.Begin(r.Context(), token, receipt)
		if err != nil {
			refuse(http.StatusForbidden)
			return
		}
		if scope.Release != nil {
			defer scope.Release()
		}
		if scope.Context == nil || scope.Release == nil || scope.Identity.Validate() != nil {
			refuse(http.StatusForbidden)
			return
		}
		// Revocation must interrupt a client that stalls while uploading a body.
		interruptDone := make(chan struct{})
		stopRead := context.AfterFunc(scope.Context, func() {
			defer close(interruptDone)
			_ = http.NewResponseController(w).SetReadDeadline(time.Now())
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now())
			_ = r.Body.Close()
		})
		defer func() {
			if !stopRead() {
				<-interruptDone
			}
		}()
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, gatewayBodyLimit))
		if err != nil || scope.Context.Err() != nil {
			refuse(http.StatusBadRequest)
			return
		}
		// Reject identity substitution before any canonical mutation or command call.
		var response any
		switch r.URL.Path {
		case "/v1/native/invoke":
			var in nativeAdmission
			if decodeStrict(raw, &in) != nil || in.Identity != scope.Identity {
				refuse(http.StatusForbidden)
				return
			}
			var result filetoolhelper.Result
			result, err = native.InvokeNative(scope.Context, providerrequest.NativeCall{Identity: in.Identity, MessageID: in.MessageID, CallID: in.CallID, Tool: in.Tool, Input: in.Input})
			if err == nil {
				err = result.Validate(in.Tool)
			}
			response = result
		case "/v1/command/invoke":
			var in commandInput
			if decodeStrict(raw, &in) != nil || in.Identity != scope.Identity {
				refuse(http.StatusForbidden)
				return
			}
			var result ManagedCommandResult
			result, err = commands.Invoke(scope.Context, ManagedCommandInput{Identity: in.Identity, MessageID: in.MessageID, CallID: in.CallID, Command: in.Command, Cwd: in.Cwd, Timeout: in.Timeout, ToolInput: in.ToolInput})
			if err == nil && (!result.Closed || result.ExecutionID == "" || len(result.Output) > commandOutputLimit) {
				err = errors.New("unverified managed command result")
			}
			response = result
		case "/v1/command/cancel":
			var in commandCancellation
			if decodeStrict(raw, &in) != nil || in.Identity != scope.Identity {
				refuse(http.StatusForbidden)
				return
			}
			err = commands.Cancel(scope.Context, in.Identity)
			response = map[string]bool{"closed": true}
		}
		if err != nil {
			refuse(http.StatusConflict)
			return
		}
		_ = json.NewEncoder(w).Encode(response)
	}), nil
}
