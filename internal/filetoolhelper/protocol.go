// Package filetoolhelper owns confined file operations for the managed profile.
package filetoolhelper

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/durationpb"
	pb "kingdom/internal/execution/openshell/managedpb"
)

const HelperPath = "/usr/local/libexec/kingdom-file-tool-helper"
const ProjectPath = "/workspace/project"
const MaxJobBytes = 1 << 20
const MaxResultBytes = 1 << 20
const MaxFileBytes = 8 << 20
const MaxEntries = 32768
const Timeout = 30 * time.Second

type Job struct {
	Version      int             `json:"version"`
	HelperSHA256 string          `json:"helper_sha256"`
	Tool         string          `json:"tool"`
	Input        json.RawMessage `json:"input"`
}

func (Job) String() string   { return "managed file operation" }
func (Job) GoString() string { return "managed file operation" }

type ToolResult struct {
	Output   any            `json:"output"`
	Content  string         `json:"content"`
	Metadata map[string]any `json:"metadata,omitempty"`
}
type Failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Effect  string `json:"effect"`
}
type Result struct {
	Version int         `json:"version"`
	Tool    string      `json:"tool"`
	OK      bool        `json:"ok"`
	Result  *ToolResult `json:"result,omitempty"`
	Error   *Failure    `json:"error,omitempty"`
}

func (r Result) Validate(tool string) error {
	if r.Version != 1 || r.Tool != tool || !knownTool(tool) || r.OK != (r.Result != nil) || r.OK == (r.Error != nil) {
		return errors.New("invalid file operation result")
	}
	if r.Error != nil && (r.Error.Code == "" || r.Error.Message == "" || (r.Error.Effect != "none" && r.Error.Effect != "partial" && r.Error.Effect != "unknown")) {
		return errors.New("invalid file operation failure")
	}
	return nil
}
func knownTool(s string) bool {
	switch s {
	case "read", "write", "edit", "patch", "glob", "grep":
		return true
	}
	return false
}
func (j Job) Validate() error {
	hash, e := hex.DecodeString(j.HelperSHA256)
	if e != nil || len(hash) != 32 || strings.ToLower(j.HelperSHA256) != j.HelperSHA256 || j.Version != 1 || !knownTool(j.Tool) {
		return errors.New("invalid file operation job")
	}
	_, e = parseInput(j.Tool, j.Input)
	return e
}
func decode(raw []byte, v any) error {
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return errors.New("invalid structured input")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	if e := unique(d, 0); e != nil {
		return e
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return errors.New("invalid structured fields")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing structured input")
	}
	return nil
}
func unique(d *json.Decoder, depth int) error {
	if depth > 16 {
		return errors.New("structured input nesting exceeded")
	}
	token, e := d.Token()
	if e != nil {
		return e
	}
	if token == nil {
		return errors.New("null structured fields are unsupported")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delim == '{' {
		seen := map[string]bool{}
		for d.More() {
			key, e := d.Token()
			name, ok := key.(string)
			if e != nil || !ok || seen[name] {
				return errors.New("duplicate structured field")
			}
			seen[name] = true
			if e = unique(d, depth+1); e != nil {
				return e
			}
		}
	} else if delim == '[' {
		for d.More() {
			if e = unique(d, depth+1); e != nil {
				return e
			}
		}
	} else {
		return errors.New("invalid structured input")
	}
	_, e = d.Token()
	return e
}
func bounded(r io.Reader, max int) ([]byte, error) {
	raw, e := io.ReadAll(io.LimitReader(r, int64(max+1)))
	if e != nil {
		return nil, e
	}
	if len(raw) > max {
		return nil, errors.New("file operation input exceeds limit")
	}
	return raw, nil
}
func DecodeJob(r io.Reader) (Job, error) {
	raw, e := bounded(r, MaxJobBytes)
	if e != nil {
		return Job{}, e
	}
	var j Job
	if e = decode(raw, &j); e != nil {
		return Job{}, e
	}
	return j, j.Validate()
}
func DecodeResult(r io.Reader, tool string) (Result, error) {
	raw, e := bounded(r, MaxResultBytes)
	if e != nil {
		return Result{}, e
	}
	var result Result
	if e = decode(raw, &result); e != nil {
		return Result{}, e
	}
	return result, result.Validate(tool)
}
func Command(j Job) (*pb.CommandSpec, error) {
	if e := j.Validate(); e != nil {
		return nil, e
	}
	raw, e := json.Marshal(j)
	if e != nil {
		return nil, e
	}
	if len(raw)+1 > MaxJobBytes {
		return nil, errors.New("file operation job exceeds limit")
	}
	return &pb.CommandSpec{Argv: []string{HelperPath, "run-v1"}, Workdir: ProjectPath, Stdin: append(raw, '\n'), Scope: pb.CommandScope_COMMAND_SCOPE_PROJECT_TOOL, ExecutionTimeout: durationpb.New(Timeout + 5*time.Second), OutputLimitBytes: MaxResultBytes}, nil
}

type input struct {
	Path                                       string
	Offset, Limit                              *int
	Content, Old, New, Patch, Pattern, Include string
	ReplaceAll, Hidden, Literal                bool
	CaseSensitive                              *bool
}

func parseInput(tool string, raw []byte) (input, error) {
	var result input
	var e error
	switch tool {
	case "read":
		var v struct {
			Path   string `json:"path"`
			Offset *int   `json:"offset,omitempty"`
			Limit  *int   `json:"limit,omitempty"`
		}
		e = decode(raw, &v)
		result.Path = v.Path
		result.Offset = v.Offset
		result.Limit = v.Limit
	case "write":
		var v struct {
			Path    *string `json:"path"`
			Content *string `json:"content"`
		}
		e = decode(raw, &v)
		if v.Path == nil || v.Content == nil {
			return result, errors.New("write fields required")
		}
		result.Path = *v.Path
		result.Content = *v.Content
	case "edit":
		var v struct {
			Path string  `json:"path"`
			Old  string  `json:"oldString"`
			New  *string `json:"newString"`
			All  bool    `json:"replaceAll,omitempty"`
		}
		e = decode(raw, &v)
		if v.New == nil || v.Old == "" || v.Old == *v.New {
			return result, errors.New("edit requires different nonempty old text")
		}
		result.Path = v.Path
		result.Old = v.Old
		result.New = *v.New
		result.ReplaceAll = v.All
	case "patch":
		var v struct {
			Text string `json:"patchText"`
		}
		e = decode(raw, &v)
		result.Patch = v.Text
		if v.Text == "" {
			return result, errors.New("patch text required")
		}
	case "glob":
		var v struct {
			Pattern string `json:"pattern"`
			Path    string `json:"path,omitempty"`
			Hidden  bool   `json:"hidden,omitempty"`
			Limit   *int   `json:"limit,omitempty"`
		}
		e = decode(raw, &v)
		result.Path = v.Path
		result.Pattern = v.Pattern
		result.Hidden = v.Hidden
		result.Limit = v.Limit
	case "grep":
		var v struct {
			Pattern string `json:"pattern"`
			Path    string `json:"path,omitempty"`
			Include string `json:"include,omitempty"`
			Literal bool   `json:"literal,omitempty"`
			Case    *bool  `json:"caseSensitive,omitempty"`
			Limit   *int   `json:"limit,omitempty"`
		}
		e = decode(raw, &v)
		result.Path = v.Path
		result.Pattern = v.Pattern
		result.Include = v.Include
		result.Literal = v.Literal
		result.CaseSensitive = v.Case
		result.Limit = v.Limit
	default:
		return result, errors.New("unsupported file operation")
	}
	if e != nil {
		return result, e
	}
	if result.Offset != nil && (*result.Offset < 0 || *result.Offset > 100000000) || result.Limit != nil && (*result.Limit < 0 || *result.Limit > 10000) {
		return result, errors.New("invalid file operation page")
	}
	if (tool == "read" || tool == "write" || tool == "edit") && result.Path == "" || (tool == "glob" || tool == "grep") && result.Pattern == "" {
		return result, errors.New("file operation field required")
	}
	return result, nil
}
