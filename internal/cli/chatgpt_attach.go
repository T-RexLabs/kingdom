package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/central/connections"
	"kingdom/internal/securefs"
	"kingdom/internal/subscription/chatgpt"
)

type chatGPTHandoffTarget struct {
	AttemptID string    `json:"attempt_id"`
	OwnerID   string    `json:"owner_id"`
	HostID    string    `json:"host_id"`
	ExpiresAt time.Time `json:"expires_at"`
}
type chatGPTAttachState struct {
	Version              int    `json:"version"`
	AttemptID            string `json:"attempt_id"`
	Service              string `json:"service"`
	OwnerID              string `json:"owner_id"`
	HostID               string `json:"host_id"`
	RegistrationID       string `json:"registration_id"`
	RegistrationRevision int64  `json:"registration_revision"`
	ConnectionID         string `json:"connection_id,omitempty"`
	ConnectionRevision   int64  `json:"connection_revision,omitempty"`
	Completed            bool   `json:"completed"`
	Acknowledged         bool   `json:"acknowledged"`
}

var newChatGPTAttachService = func(ctx context.Context, vault *chatgpt.Vault) (*chatgpt.Service, error) {
	return chatgpt.NewService(ctx, vault, nil)
}
var saveChatGPTAttachState = func(root *securefs.Root, state chatGPTAttachState) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return root.ReplaceRegular("state.json", encoded, 0600, 0700, time.Time{})
}

func attachUUID(raw string) bool {
	id, err := uuid.Parse(raw)
	return err == nil && id != uuid.Nil && id.String() == raw
}

func readChatGPTAttachState(root *securefs.Root) ([]byte, error) {
	file, err := root.OpenRegular("state.json")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Mode().Perm() != 0600 || securefs.ValidateCurrentUserOnly(file) != nil {
		return nil, chatgpt.ErrDenied
	}
	raw, err := io.ReadAll(io.LimitReader(file, 8193))
	if err != nil || len(raw) > 8192 {
		return nil, chatgpt.ErrDenied
	}
	return raw, nil
}

var readChatGPTPairingCode = func(out io.Writer) (string, error) {
	fmt.Fprint(out, "Pairing Code: ")
	line, err := bufio.NewReader(io.LimitReader(os.Stdin, 128)).ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	code := strings.TrimSpace(line)
	if len(code) != 43 {
		return "", errors.New("enter the pairing code from Kingdom Connections")
	}
	return code, nil
}

func chatGPTServiceOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" && u.Path != "/" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") || u.Opaque != "" {
		return "", errors.New("the Kingdom service must be an HTTPS origin")
	}
	return "https://" + u.Host, nil
}
func chatGPTHandoffRequest(ctx context.Context, client *http.Client, origin, code, action string, transfer json.RawMessage, out any) error {
	body, err := json.Marshal(struct {
		Action   string          `json:"action"`
		Transfer json.RawMessage `json:"transfer,omitempty"`
	}{action, transfer})
	if err != nil {
		return chatgpt.ErrInvalid
	}
	defer clear(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, origin+"/api/central/v1/personal-accounts/handoff", bytes.NewReader(body))
	if err != nil {
		return chatgpt.ErrInvalid
	}
	req.Header.Set("Authorization", "Bearer "+code)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return errors.New("the Kingdom service could not be reached; retry with the same pairing code")
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return errors.New("the Kingdom service rejected the handoff; check that the sign-in attempt and browser session are still active")
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, (256<<10)+1))
	if err != nil || len(raw) > 256<<10 {
		return chatgpt.ErrUnavailable
	}
	defer clear(raw)
	if json.Unmarshal(raw, out) != nil {
		return chatgpt.ErrUnavailable
	}
	return nil
}
func privateAttachRoot(path string) (*securefs.Root, error) {
	if !filepath.IsAbs(path) {
		return nil, chatgpt.ErrInvalid
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, chatgpt.ErrDenied
	}
	owned, err := os.Open(path)
	if err != nil {
		return nil, chatgpt.ErrDenied
	}
	defer owned.Close()
	actual, err := owned.Stat()
	if err != nil || !os.SameFile(info, actual) || securefs.ValidateCurrentUserOnly(owned) != nil {
		return nil, chatgpt.ErrDenied
	}
	return securefs.OpenRoot(path)
}
func runChatGPTAttach(ctx context.Context, out io.Writer, args []string) (resultErr error) {
	f := flag.NewFlagSet("chatgpt attach", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	service := f.String("service", "", "Kingdom HTTPS origin")
	caFile := f.String("ca-file", "", "Operator-provided certificate authority")
	stateRoot := f.String("state-root", "", "Protected helper state directory")
	if err := f.Parse(args); err != nil || f.NArg() != 0 {
		return errors.New("use kd chatgpt attach --service HTTPS_ORIGIN")
	}
	origin, err := chatGPTServiceOrigin(*service)
	if err != nil {
		return err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if *caFile != "" {
		pool, e := x509.SystemCertPool()
		if e != nil {
			return chatgpt.ErrInvalid
		}
		raw, e := os.ReadFile(*caFile)
		if e != nil || len(raw) > 1<<20 || !pool.AppendCertsFromPEM(raw) {
			return chatgpt.ErrInvalid
		}
		transport.TLSClientConfig.RootCAs = pool
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	fmt.Fprintf(out, "Connect your personal ChatGPT account to %s.\n", origin)
	code, err := readChatGPTPairingCode(out)
	if err != nil {
		return err
	}
	var target chatGPTHandoffTarget
	if err = chatGPTHandoffRequest(ctx, client, origin, code, "prepare", nil, &target); err != nil {
		return err
	}
	for _, id := range []string{target.AttemptID, target.OwnerID, strings.TrimPrefix(target.HostID, "urn:uuid:")} {
		parsed, e := uuid.Parse(id)
		if e != nil || parsed == uuid.Nil || parsed.String() != id {
			return chatgpt.ErrDenied
		}
	}
	if !strings.HasPrefix(target.HostID, "urn:uuid:") || !time.Now().Before(target.ExpiresAt) {
		return chatgpt.ErrDenied
	}
	ctx, cancel := context.WithDeadline(ctx, target.ExpiresAt)
	defer cancel()
	if *stateRoot == "" {
		base, e := os.UserConfigDir()
		if e != nil {
			return e
		}
		*stateRoot = filepath.Join(base, "kingdom", "account-handoffs")
	}
	digest := sha256.Sum256([]byte(origin))
	profile := filepath.Join(*stateRoot, hex.EncodeToString(digest[:16]))
	root, err := privateAttachRoot(profile)
	if err != nil {
		return err
	}
	defer root.Close()
	key, err := root.ReadRegular("key", 32)
	if errors.Is(err, os.ErrNotExist) {
		key = make([]byte, 32)
		if _, err = rand.Read(key); err != nil {
			return err
		}
		if _, err = root.CreateRegularExact("key", key, 0600, 0700); err != nil {
			return err
		}
	}
	if err != nil || len(key) != 32 {
		return chatgpt.ErrDenied
	}
	defer clear(key)
	keyFile, err := root.OpenRegular("key")
	if err != nil {
		return err
	}
	info, err := keyFile.Stat()
	ownerErr := securefs.ValidateCurrentUserOnly(keyFile)
	keyFile.Close()
	if err != nil || info.Mode().Perm() != 0600 || ownerErr != nil {
		return chatgpt.ErrDenied
	}
	attemptRoot := filepath.Join(profile, target.AttemptID)
	private, err := privateAttachRoot(attemptRoot)
	if err != nil {
		return err
	}
	defer private.Close()
	storage, err := chatgpt.OpenFileVault(ctx, profile, key)
	if err != nil {
		return err
	}
	account, err := newChatGPTAttachService(ctx, storage.Vault)
	if err != nil {
		storage.Close()
		return err
	}
	defer func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := account.Close(closeCtx); err != nil {
			resultErr = err
			return
		}
		if err := storage.Close(); err != nil {
			resultErr = err
		}
	}()
	state := chatGPTAttachState{Version: 1, AttemptID: target.AttemptID, Service: origin, OwnerID: target.OwnerID, HostID: target.HostID}
	previous, readErr := readChatGPTAttachState(private)
	if readErr == nil {
		decoder := json.NewDecoder(bytes.NewReader(previous))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&state) != nil || decoder.Decode(new(any)) != io.EOF || state.Version != 1 || state.AttemptID != target.AttemptID || state.Service != origin || state.OwnerID != target.OwnerID || state.HostID != target.HostID || state.RegistrationID != "" && !attachUUID(state.RegistrationID) || state.RegistrationRevision < 0 || state.Completed && (state.RegistrationRevision < 1 || !attachUUID(state.ConnectionID) || state.ConnectionRevision < 1) || state.Acknowledged && !state.Completed {
			return chatgpt.ErrDenied
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return chatgpt.ErrDenied
	}
	if state.Acknowledged {
		fmt.Fprintln(out, "This personal account was already attached.")
		return nil
	}
	var transfer chatgpt.Transfer
	haveTransfer := false
	if state.RegistrationID != "" {
		reg, e := account.Registration(ctx, target.OwnerID, state.RegistrationID)
		if e != nil {
			return e
		}
		if state.RegistrationRevision != 0 && reg.Revision != state.RegistrationRevision {
			return errors.New("the selected local authorization changed; check this handoff in Kingdom Connections")
		}
		if reg.State == "signed_out" && state.Completed {
			// The vault acknowledgement committed but its state file write may
			// have failed. The durable completion receipt permits local recovery
			// without starting a new authorization or sending another session.
			state.Acknowledged = true
			if err = saveChatGPTAttachState(private, state); err != nil {
				return err
			}
			fmt.Fprintln(out, "This personal account was already attached.")
			return nil
		}
		if reg.State == "ready" || reg.State == "reauth" {
			// Recover the exact previously fenced transfer after a lost response.
			// Never reauthorize over a source session already handed to the VM.
			if state.RegistrationRevision == 0 {
				state.RegistrationRevision = reg.Revision
				if err = saveChatGPTAttachState(private, state); err != nil {
					return err
				}
			}
			transfer, err = account.ExportForTransfer(ctx, target.OwnerID, state.RegistrationID, target.HostID)
			if err != nil {
				return err
			}
			haveTransfer = true
		} else if reg.State != "pending" && reg.State != "exchanging" {
			return errors.New("this handoff has ended; check its result in Kingdom Connections")
		}
	}
	if !haveTransfer {
		flow, e := account.Begin(ctx, target.OwnerID, state.RegistrationID)
		if e != nil {
			return e
		}
		defer flow.Close()
		state.RegistrationID = flow.RegistrationID()
		if err = saveChatGPTAttachState(private, state); err != nil {
			return err
		}
		if err = openChatGPTBrowser(ctx, flow.AuthorizationURL()); err != nil {
			return err
		}
		fmt.Fprintln(out, "Complete ChatGPT sign-in in your browser.")
		reg, err := flow.Wait(ctx)
		if err != nil {
			return err
		}
		state.RegistrationRevision = reg.Revision
		if err = saveChatGPTAttachState(private, state); err != nil {
			return err
		}
		transfer, err = account.ExportForTransfer(ctx, target.OwnerID, state.RegistrationID, target.HostID)
		if err != nil {
			return err
		}
	}
	payload, err := chatgpt.EncodeTransfer(transfer)
	if err != nil {
		return err
	}
	defer clear(payload)
	var result struct {
		ID         string                `json:"id"`
		Provider   string                `json:"provider"`
		Status     string                `json:"status"`
		Connection *connections.Metadata `json:"connection"`
	}
	if err = chatGPTHandoffRequest(ctx, client, origin, code, "complete", payload, &result); err != nil {
		return err
	}
	if result.ID != target.AttemptID || result.Provider != "openai" || result.Status != "complete" || result.Connection == nil || !attachUUID(result.Connection.ID) || result.Connection.Revision < 1 || result.Connection.OwnerID != target.OwnerID || result.Connection.Scope != connections.Personal || result.Connection.Disabled || result.Connection.Definition.Provider != "openai" || result.Connection.Definition.AuthKind != "openai_oauth" || result.Connection.Definition.Endpoint != "" {
		return chatgpt.ErrDenied
	}
	state.Completed = true
	state.ConnectionID = result.Connection.ID
	state.ConnectionRevision = result.Connection.Revision
	// Retain the verified result before removing source token copies. If either
	// later write is interrupted, retry can recover without replacing a session.
	if err = saveChatGPTAttachState(private, state); err != nil {
		return err
	}
	if err = account.AcknowledgeTransfer(ctx, target.OwnerID, state.RegistrationID, transfer); err != nil {
		return err
	}
	state.Acknowledged = true
	if err = saveChatGPTAttachState(private, state); err != nil {
		return err
	}
	fmt.Fprintln(out, "Your personal ChatGPT account is connected. Kingdom will manage later token refreshes.")
	return nil
}
