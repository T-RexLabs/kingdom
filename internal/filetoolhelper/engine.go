package filetoolhelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"

	"kingdom/internal/securefs"
	"kingdom/internal/trackedfs"
)

type operationError struct{ code, message, effect string }

func (e *operationError) Error() string { return e.message }
func fail(code, message string) error   { return &operationError{code, message, "none"} }
func unsupported(message string) error  { return fail("unsupported", message) }
func resultError(tool string, e error) Result {
	failure := &Failure{Code: "io_failed", Message: "The file operation failed.", Effect: "none"}
	var known *operationError
	if errors.As(e, &known) {
		failure = &Failure{Code: known.code, Message: known.message, Effect: known.effect}
	}
	return Result{Version: 1, Tool: tool, Error: failure}
}
func Run(ctx context.Context, j Job, out io.Writer) error {
	if e := j.Validate(); e != nil {
		return e
	}
	imageRoot, e := securefs.OpenRoot("/")
	if e != nil {
		return e
	}
	defer imageRoot.Close()
	if e := verifyExecutable(imageRoot, j.HelperSHA256); e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	result := execute(ctx, ProjectPath, j)
	raw, e := json.Marshal(result)
	if e != nil {
		return e
	}
	_, e = out.Write(append(raw, '\n'))
	return e
}
func execute(ctx context.Context, rootPath string, j Job) Result {
	if e := j.Validate(); e != nil {
		return resultError(j.Tool, fail("invalid_input", "Invalid file operation input."))
	}
	in, _ := parseInput(j.Tool, j.Input)
	root, e := securefs.OpenRoot(rootPath)
	if e != nil {
		return resultError(j.Tool, e)
	}
	defer root.Close()
	var output *ToolResult
	var commit func() error
	switch j.Tool {
	case "read":
		output, e = read(root, in)
	case "glob", "grep":
		output, e = search(ctx, root, j.Tool, in)
	case "write", "edit":
		output, commit, e = mutation(root, j.Tool, in)
	case "patch":
		output, commit, e = patch(root, in.Patch)
	}
	if e != nil {
		return resultError(j.Tool, e)
	}
	result := Result{Version: 1, Tool: j.Tool, OK: true, Result: output}
	encoded, e := json.Marshal(result)
	if e != nil || len(encoded)+1 > MaxResultBytes {
		return resultError(j.Tool, fail("limit_exceeded", "The file operation result exceeds its limit."))
	}
	if e = ctx.Err(); e != nil {
		return resultError(j.Tool, fail("cancelled", "The file operation was cancelled."))
	}
	if commit != nil {
		if e = commit(); e != nil {
			return resultError(j.Tool, e)
		}
	}
	return result
}
func relative(value string) (string, error) {
	if value == "" {
		value = "."
	}
	if strings.HasPrefix(value, ProjectPath+"/") {
		value = strings.TrimPrefix(value, ProjectPath+"/")
	} else if value == ProjectPath {
		value = "."
	}
	if len(value) > 4096 || !utf8.ValidString(value) || strings.HasPrefix(value, "/") || strings.Contains(value, "\\") || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", fail("invalid_path", "The path is outside the project.")
	}
	parts := strings.Split(value, "/")
	if len(parts) > 64 {
		return "", fail("limit_exceeded", "The path depth exceeds its limit.")
	}
	for _, part := range parts {
		if part == ".." || part != "." && strings.TrimRight(part, ". ") != part {
			return "", fail("invalid_path", "The path contains an unsupported component.")
		}
		if trackedfs.IsProtectedRelativePath(strings.ToLower(part)) || strings.HasPrefix(strings.ToLower(part), ".kingdom-") {
			return "", fail("protected_path", "The path contains protected metadata.")
		}
	}
	return path.Clean(value), nil
}
func text(root *securefs.Root, name string) ([]byte, securefs.FileIdentity, error) {
	raw, id, e := root.ReadRegularIdentified(name, MaxFileBytes)
	if e != nil {
		return nil, id, e
	}
	if !utf8.Valid(raw) || strings.ContainsRune(string(raw), 0) {
		return nil, id, unsupported("Only UTF-8 text files are supported.")
	}
	return raw, id, nil
}

func verifyExecutable(imageRoot *securefs.Root, expected string) error {
	file, e := imageRoot.OpenRegular(strings.TrimPrefix(HelperPath, "/"))
	if e != nil {
		return e
	}
	info, e := file.Stat()
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		file.Close()
		return errors.New("file helper executable unavailable")
	}
	h := sha256.New()
	_, e = io.Copy(h, file)
	closeErr := file.Close()
	if e != nil || closeErr != nil {
		return errors.New("file helper identity unavailable")
	}
	if hex.EncodeToString(h.Sum(nil)) != expected {
		return errors.New("file helper identity mismatch")
	}
	return nil
}
