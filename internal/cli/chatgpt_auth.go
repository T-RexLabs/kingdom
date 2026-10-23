package cli

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"kingdom/internal/execution/openshell/localowner"
	"kingdom/internal/execution/openshell/localstack"
	"kingdom/internal/securefs"
	"kingdom/internal/subscription/chatgpt"
)

type chatGPTCredentials struct {
	*localowner.ChatGPT
	service *chatgpt.Service
	storage *chatgpt.FileVault
}

func (c *chatGPTCredentials) Close(ctx context.Context) error {
	if e := c.service.Close(ctx); e != nil {
		return e
	}
	return c.storage.Close()
}

func credentialsFactory(config localstack.Config) localstack.CredentialsFactory {
	return func(ctx context.Context, db *sql.DB, key []byte, owner, project string, model localstack.ModelConfig) (localstack.Credentials, error) {
		if model.AuthKind == "api_key" {
			return localstack.APIKeyCredentials(ctx, db, key, owner, project, model)
		}
		if model.AuthKind != "chatgpt" || model.CAFile != "" || len(model.AllowedPrivateAddresses) != 0 {
			return nil, localstack.ErrConfig
		}
		storage, e := chatgpt.OpenFileVault(ctx, config.StateRoot, key)
		if e != nil {
			return nil, e
		}
		service, e := chatgpt.NewService(ctx, storage.Vault, nil)
		if e != nil {
			storage.Close()
			return nil, e
		}
		adapter, e := localowner.NewChatGPT(service, owner, project, model.RegistrationID, nil)
		if e != nil {
			service.Close(ctx)
			storage.Close()
			return nil, e
		}
		return &chatGPTCredentials{adapter, service, storage}, nil
	}
}

var openChatGPTBrowser = func(ctx context.Context, url string) error {
	var name string
	switch runtime.GOOS {
	case "darwin":
		name = "open"
	case "linux":
		name = "xdg-open"
	default:
		return errors.New("browser sign-in is unavailable on this platform")
	}
	// The authorization URL is passed directly to the OS browser, never through
	// a shell or to stdout, logs, persisted history, or an upstream callback.
	if e := exec.CommandContext(ctx, name, url).Run(); e != nil {
		return errors.New("could not open the browser for sign-in")
	}
	return nil
}

func readChatGPTKey(path, owner string) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, chatgpt.ErrInvalid
	}
	root, e := securefs.OpenRoot(filepath.Dir(path))
	if e != nil {
		return nil, chatgpt.ErrInvalid
	}
	defer root.Close()
	f, e := root.OpenRegular(filepath.Base(path))
	if e != nil {
		return nil, chatgpt.ErrInvalid
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil || info.Mode().Perm() != 0600 || info.Size() > 128 {
		return nil, chatgpt.ErrInvalid
	}
	raw, e := io.ReadAll(io.LimitReader(f, 129))
	if e != nil {
		return nil, chatgpt.ErrInvalid
	}
	defer clear(raw)
	master, e := hex.DecodeString(strings.TrimSpace(string(raw)))
	if e != nil || len(master) != 32 {
		return nil, chatgpt.ErrInvalid
	}
	defer clear(master)
	return localstack.OwnerCredentialKey(master, owner), nil
}
func runChatGPTCommand(ctx context.Context, out io.Writer, args []string) (resultErr error) {
	if len(args) > 0 && (args[0] == "--help" || args[0] == "help" || (len(args) > 1 && args[1] == "--help")) {
		fmt.Fprintln(out, "Usage: kd chatgpt sign-in|status|disconnect --state-root PATH --owner-id UUID --master-key-file PATH [--registration-id UUID]")
		fmt.Fprintln(out, "Connect your own ChatGPT account for local OpenShell execution. A shared hosted service requires a separately approved client.")
		return nil
	}
	if len(args) == 0 {
		return errors.New("use chatgpt sign-in, status, or disconnect")
	}
	action := args[0]
	if action != "sign-in" && action != "status" && action != "disconnect" {
		return errors.New("unknown ChatGPT command")
	}
	flags := flag.NewFlagSet("chatgpt "+action, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	root := flags.String("state-root", "", "Protected local OpenShell state directory")
	owner := flags.String("owner-id", "", "Local owner ID")
	keyFile := flags.String("master-key-file", "", "Protected master key file")
	registration := flags.String("registration-id", "", "Kingdom ChatGPT registration ID")
	if e := flags.Parse(args[1:]); e != nil || flags.NArg() != 0 {
		return errors.New("invalid ChatGPT command flags")
	}
	ownerUUID, ownerErr := uuid.Parse(*owner)
	if ownerErr != nil || ownerUUID.String() != *owner || ownerUUID == uuid.Nil {
		return chatgpt.ErrInvalid
	}
	if !filepath.IsAbs(*root) || *owner == "" || action != "sign-in" && *registration == "" {
		return chatgpt.ErrInvalid
	}
	key, e := readChatGPTKey(*keyFile, *owner)
	if e != nil {
		return e
	}
	defer clear(key)
	storage, e := chatgpt.OpenFileVault(ctx, *root, key)
	if e != nil {
		return e
	}
	if action == "status" {
		defer storage.Close()
		reg, e := storage.Get(ctx, *owner, *registration)
		if e != nil {
			return e
		}
		fmt.Fprintf(out, "Registration: %s\nState: %s\n", reg.ID, reg.State)
		return nil
	}
	service, e := chatgpt.NewService(ctx, storage.Vault, &http.Client{})
	if e != nil {
		storage.Close()
		return e
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if e := service.Close(closeCtx); e != nil {
			resultErr = e
			return
		}
		if e := storage.Close(); e != nil && resultErr == nil {
			resultErr = e
		}
	}()
	if action == "disconnect" {
		confirmed, e := service.Disconnect(ctx, *owner, *registration)
		if !confirmed {
			return errors.New("local use is blocked; remote revocation is not confirmed. Remove the Kingdom connection in ChatGPT account settings")
		}
		return e
	}
	flow, e := service.Begin(ctx, *owner, *registration)
	if e != nil {
		return e
	}
	defer flow.Close()
	fmt.Fprintf(out, "Registration: %s\nOpening ChatGPT sign-in in your browser.\n", flow.RegistrationID())
	if e = openChatGPTBrowser(ctx, flow.AuthorizationURL()); e != nil {
		return e
	}
	reg, e := flow.Wait(ctx)
	if e != nil {
		return e
	}
	fmt.Fprintf(out, "ChatGPT connected. Set model.auth_kind to chatgpt and model.registration_id to %s in the local OpenShell configuration.\n", reg.ID)
	return nil
}
