package opencode

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
)

//go:embed plugin/index.js plugin/package.json
var pluginSource embed.FS

// installPlugin writes the embedded Kingdom plugin as a local package. OpenCode
// watches the package, so unchanged files are left alone to avoid a reload.
func installPlugin(directory string) error {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create OpenCode plugin directory: %w", err)
	}
	for _, name := range []string{"index.js", "package.json"} {
		want, err := pluginSource.ReadFile("plugin/" + name)
		if err != nil {
			return err
		}
		target := filepath.Join(directory, name)
		if have, err := os.ReadFile(target); err == nil && bytes.Equal(have, want) {
			continue
		}
		temporary := target + ".tmp"
		if err := os.WriteFile(temporary, want, 0o600); err != nil {
			return fmt.Errorf("write OpenCode plugin: %w", err)
		}
		if err := os.Rename(temporary, target); err != nil {
			return fmt.Errorf("install OpenCode plugin: %w", err)
		}
	}
	return nil
}

// recoverReceipts delivers native receipts that the plugin persisted but could
// not deliver, then removes them. Delivery is idempotent, so a receipt the
// plugin delivers concurrently is harmless. A non-nil only limits recovery to
// one request, so a receipt that cannot be delivered never blocks another.
func recoverReceipts(ctx context.Context, outbox string, authority *providerrequest.Authority, only *providerexecution.RequestIdentity) error {
	entries, err := os.ReadDir(outbox)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	var failures []error
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(outbox, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				failures = append(failures, err)
			}
			continue
		}
		var receipt nativeReceipt
		if err := decodeStrict(raw, &receipt); err != nil {
			failures = append(failures, fmt.Errorf("native receipt %s: %w", entry.Name(), err))
			continue
		}
		if only != nil && receipt.Identity != *only {
			continue
		}
		if err := authority.FinishNative(ctx, receipt.Identity, receipt.providerReceipt()); err != nil {
			failures = append(failures, fmt.Errorf("native receipt %s: %w", entry.Name(), err))
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// withPlugin adds the Kingdom plugin to the inline configuration's plugin list,
// preserving the user's own plugins.
func withPlugin(config map[string]json.RawMessage, directory string) error {
	var plugins []string
	if raw, ok := config["plugins"]; ok {
		if err := json.Unmarshal(raw, &plugins); err != nil {
			return fmt.Errorf("OpenCode inline configuration plugins must be a list of paths")
		}
	}
	for _, existing := range plugins {
		if existing == directory {
			return nil
		}
	}
	encoded, err := json.Marshal(append(plugins, directory))
	if err != nil {
		return err
	}
	config["plugins"] = encoded
	return nil
}

const pluginID = "kingdom.runtime"

// awaitPlugin returns once the Kingdom plugin is active at the location, and
// reports why it failed to load otherwise, so a broken installation surfaces at
// startup rather than at the first request.
func awaitPlugin(ctx context.Context, client *Client, directory string) error {
	for {
		plugins, err := client.Plugins(ctx, directory)
		if err != nil {
			return err
		}
		for _, plugin := range plugins {
			if plugin.ID != pluginID {
				continue
			}
			switch plugin.State.Status {
			case "active":
				return nil
			case "failed":
				return fmt.Errorf("the Kingdom plugin failed to load (%s); restart Kingdom and check for conflicting OpenCode plugins", plugin.State.Error)
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("the Kingdom plugin did not load: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
