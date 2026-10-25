package deployment

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

type ExportOptions struct {
	DockerContext, GatewayImage, WorkerImage string
	Profile                                  Profile
	Environment                              Environment
}

var containerIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type dockerCLI struct{ context string }

func (d dockerCLI) command(ctx context.Context, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, "docker", append([]string{"--context", d.context}, args...)...)
}
func (d dockerCLI) output(ctx context.Context, args ...string) ([]byte, error) {
	var out bytes.Buffer
	c := d.command(ctx, args...)
	c.Stdout = &boundedOutput{writer: &out, left: 256 << 10}
	if c.Run() != nil {
		return nil, fmt.Errorf("Docker artifact inspection failed")
	}
	return out.Bytes(), nil
}

type boundedOutput struct {
	writer io.Writer
	left   int
}

func (w *boundedOutput) Write(p []byte) (int, error) {
	if len(p) > w.left {
		return 0, ErrInvalid
	}
	n, e := w.writer.Write(p)
	w.left -= n
	return n, e
}
func artifactDigest(in io.Reader, name string) (string, error) {
	r := tar.NewReader(in)
	h, e := r.Next()
	if e != nil || h.Typeflag != tar.TypeReg || h.Name != name || h.Size < 1 || h.Size > 512<<20 {
		return "", ErrInvalid
	}
	hash := sha256.New()
	n, e := io.Copy(hash, io.LimitReader(r, h.Size+1))
	if e != nil || n != h.Size {
		return "", ErrInvalid
	}
	if _, e = r.Next(); e != io.EOF {
		return "", ErrInvalid
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
func (d dockerCLI) digest(ctx context.Context, id, path string) (string, error) {
	c := d.command(ctx, "cp", id+":"+path, "-")
	pipe, e := c.StdoutPipe()
	if e != nil {
		return "", e
	}
	if c.Start() != nil {
		return "", fmt.Errorf("Docker artifact copy failed")
	}
	name := path[strings.LastIndex(path, "/")+1:]
	digest, e := artifactDigest(pipe, name)
	if e != nil {
		_ = c.Process.Kill()
	}
	_ = pipe.Close()
	if wait := c.Wait(); e == nil && wait != nil {
		e = fmt.Errorf("Docker artifact copy failed")
	}
	return digest, e
}
func (d dockerCLI) image(ctx context.Context, pin string) error {
	raw, e := d.output(ctx, "image", "inspect", pin)
	if e != nil {
		return e
	}
	var images []struct {
		Architecture, Os string
		RepoDigests      []string
	}
	if json.Unmarshal(raw, &images) != nil || len(images) != 1 || images[0].Architecture != "arm64" || images[0].Os != "linux" {
		return ErrInvalid
	}
	for _, v := range images[0].RepoDigests {
		if v == pin {
			return nil
		}
	}
	return ErrInvalid
}

// Export inspects only explicitly pinned local images. Containers created for
// fixed-path artifact reads are never started and are removed by exact ID.
func Export(ctx context.Context, o ExportOptions) (b Bundle, resultErr error) {
	if o.DockerContext == "" || strings.HasPrefix(o.DockerContext, "-") || strings.ContainsAny(o.DockerContext, "\x00\r\n") {
		return b, ErrInvalid
	}
	b = Bundle{Kind: Kind, Version: 1, Gateway: o.Environment.Gateway, Profile: o.Profile, ProfileSHA256: o.Profile.Digest(), WorkerID: o.Environment.WorkerID, Callback: o.Environment.Callback, Limits: o.Environment.Limits}
	b.Gateway.Image = o.GatewayImage
	b.Worker.Image = o.WorkerImage
	for _, image := range []string{o.GatewayImage, o.WorkerImage, o.Profile.SupervisorImage, o.Profile.SandboxRuntimeImage} {
		if !imagePattern.MatchString(image) {
			return Bundle{}, ErrInvalid
		}
	}
	if o.Profile.SchemaVersion != 1 || !hashValid(o.Profile.GatewayBinarySHA256) || !hashValid(o.Profile.SandboxRuntimeBinarySHA256) {
		return Bundle{}, ErrInvalid
	}
	d := dockerCLI{o.DockerContext}
	for _, pin := range []string{o.GatewayImage, o.WorkerImage, o.Profile.SupervisorImage, o.Profile.SandboxRuntimeImage} {
		if e := d.image(ctx, pin); e != nil {
			return Bundle{}, e
		}
	}
	ids := []string{}
	defer func() {
		for _, id := range ids {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, e := d.output(cleanup, "rm", "--volumes", id)
			cancel()
			if e != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("stopped export container cleanup failed; retain exact ID %s", id))
			}
		}
	}()
	create := func(pin string) (string, error) {
		operation := uuid.NewString()
		name := "kingdom-openshell-export-" + operation
		raw, e := d.output(ctx, "create", "--pull=never", "--name", name, "--label", "kingdom.openshell.export="+operation, "--entrypoint", "/kingdom-export-never-run", pin)
		id := strings.TrimSpace(string(raw))
		if e != nil || !containerIDPattern.MatchString(id) {
			// A lost or malformed create reply cannot authorize another create. Resolve only
			// this operation's exact named stopped container, then remove its ID.
			resolve, stopResolve := context.WithTimeout(context.Background(), 10*time.Second)
			inspect, readErr := d.output(resolve, "container", "inspect", name)
			stopResolve()
			var found []struct {
				ID     string
				Config struct {
					Image  string
					Labels map[string]string
				}
				State struct{ Running bool }
			}
			if readErr == nil && json.Unmarshal(inspect, &found) == nil && len(found) == 1 && found[0].Config.Image == pin && found[0].Config.Labels["kingdom.openshell.export"] == operation && !found[0].State.Running && containerIDPattern.MatchString(found[0].ID) {
				ids = append(ids, found[0].ID)
			}
			return "", fmt.Errorf("Docker export creation unresolved for operation %s; no create was repeated", name)
		}
		ids = append(ids, id)
		return id, nil
	}
	gateway, e := create(o.GatewayImage)
	if e != nil {
		return Bundle{}, e
	}
	gatewayHash, e := d.digest(ctx, gateway, "/usr/local/bin/openshell-gateway")
	if e != nil || gatewayHash != o.Profile.GatewayBinarySHA256 {
		return Bundle{}, ErrInvalid
	}
	runtime, e := create(o.Profile.SandboxRuntimeImage)
	if e != nil {
		return Bundle{}, e
	}
	runtimeHash, e := d.digest(ctx, runtime, "/openshell-sandbox")
	if e != nil || runtimeHash != o.Profile.SandboxRuntimeBinarySHA256 {
		return Bundle{}, ErrInvalid
	}
	worker, e := create(o.WorkerImage)
	if e != nil {
		return Bundle{}, e
	}
	for _, item := range []struct {
		path   string
		target *string
	}{{"/usr/local/libexec/kingdom-provider-launcher", &b.Worker.LauncherSHA256}, {"/usr/local/bin/opencode", &b.Worker.OpenCodeSHA256}, {"/usr/local/libexec/kingdom-workspace-helper", &b.Worker.WorkspaceHelperSHA256}, {"/usr/local/libexec/kingdom-file-tool-helper", &b.Worker.FileToolSHA256}} {
		*item.target, e = d.digest(ctx, worker, item.path)
		if e != nil {
			return Bundle{}, e
		}
	}
	if b.Validate() != nil {
		return Bundle{}, ErrInvalid
	}
	return b, nil
}
