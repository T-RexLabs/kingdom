package opencode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"kingdom/internal/processguardian"
)

// LocalExecutor owns the local filesystem and process operations used by both
// OpenCode and its managed shell commands. All paths and guardian identities
// belong to the Kingdom host. This is not a remote execution interface: a remote
// process ID or the exit of a transport client cannot satisfy its ownership and
// quiescence contract.
type LocalExecutor interface {
	Start(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error)
	ResolveWorkspace(string) (string, error)
	ResolveCommandDirectory(workspace, directory string) (string, error)
}

// localExecutor retains the existing host guardian and filesystem behavior.
type localExecutor struct {
	runner *processguardian.Runner
}

func (e localExecutor) Start(ctx context.Context, command processguardian.Command, persist processguardian.PersistOwnership) (processguardian.Handle, error) {
	return e.runner.Start(ctx, command, persist)
}

func (localExecutor) ResolveWorkspace(directory string) (string, error) {
	return filepath.EvalSymlinks(filepath.Clean(directory))
}

// ResolveCommandDirectory permits existing directories inside the workspace,
// including aliases that resolve there, without allowing symlinks to escape.
func (localExecutor) ResolveCommandDirectory(workspace, directory string) (string, error) {
	root, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return "", fmt.Errorf("resolve managed command workspace: %w", err)
	}
	if !filepath.IsAbs(directory) {
		directory = filepath.Join(root, directory)
	}
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return "", fmt.Errorf("resolve managed command directory: %w", err)
	}
	relative, err := filepath.Rel(root, resolved)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("a managed command must run within its workspace %s", workspace)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("inspect managed command directory: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("managed command directory is not a directory: %s", directory)
	}
	return resolved, nil
}
