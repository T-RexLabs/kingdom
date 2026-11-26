package filetoolhelper

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"kingdom/internal/securefs"
)

type change struct {
	name            string
	before, after   []byte
	identity        securefs.FileIdentity
	existed, remove bool
	mode            os.FileMode
}

func prepare(root *securefs.Root, name string) (change, error) {
	relative, e := relative(name)
	if e != nil {
		return change{}, e
	}
	if relative == "." {
		return change{}, unsupported("A regular file path is required.")
	}
	c := change{name: relative, mode: 0644}
	parent := path.Dir(relative)
	for parent != "." {
		entry, e := root.ReadEntry(parent)
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			return c, e
		}
		if e == nil && entry.Type != securefs.EntryTypeDirectory {
			return c, unsupported("Symbolic links and non-directory parents are not supported.")
		}
		parent = path.Dir(parent)
	}
	raw, id, e := text(root, relative)
	if errors.Is(e, os.ErrNotExist) {
		return c, nil
	}
	if e != nil {
		return c, e
	}
	file, e := root.OpenRegular(relative)
	if e != nil {
		return c, e
	}
	info, e := file.Stat()
	file.Close()
	if e != nil {
		return c, e
	}
	c.mode = info.Mode().Perm()
	c.before = raw
	c.identity = id
	c.existed = true
	return c, nil
}
func check(root *securefs.Root, c change) error {
	current, id, e := text(root, c.name)
	if !c.existed {
		if errors.Is(e, os.ErrNotExist) {
			return nil
		}
		return fail("precondition_failed", "The file changed before publication.")
	}
	if e != nil || id != c.identity || !bytes.Equal(current, c.before) {
		return fail("precondition_failed", "The file changed before publication.")
	}
	return nil
}

// Publication relies on the canonical allocation-wide exclusive tool admission.
// The kernel project scope is independently required for every helper command.
func publish(root *securefs.Root, changes []change) error {
	for _, c := range changes {
		if e := check(root, c); e != nil {
			return e
		}
	}
	applied := 0
	for _, c := range changes {
		if e := check(root, c); e != nil {
			if applied > 0 {
				return &operationError{"precondition_failed", "A file changed after earlier patch files were published.", "partial"}
			}
			return e
		}
		var e error
		if c.remove {
			e = root.RemoveRegularIfIdentity(c.name, c.identity)
			if e == nil {
				e = root.SyncDir(path.Dir(c.name))
			}
		} else {
			e = root.ReplaceRegular(c.name, c.after, c.mode, 0755, time.Time{})
		}
		if e != nil {
			effect := "unknown"
			if applied > 0 {
				effect = "partial"
			}
			return &operationError{"publication_failed", "File publication failed; the project must be verified before reuse.", effect}
		}
		applied++
	}
	return nil
}
func mutation(root *securefs.Root, tool string, in input) (*ToolResult, func() error, error) {
	c, e := prepare(root, in.Path)
	if e != nil {
		return nil, nil, e
	}
	before := strings.TrimPrefix(string(c.before), "\ufeff")
	bom := strings.HasPrefix(string(c.before), "\ufeff")
	var result *ToolResult
	if tool == "write" {
		next := strings.TrimPrefix(in.Content, "\ufeff")
		if bom || strings.HasPrefix(in.Content, "\ufeff") {
			next = "\ufeff" + next
		}
		c.after = []byte(next)
		word := "Created"
		if c.existed {
			word = "Wrote"
		}
		result = &ToolResult{Output: map[string]any{"operation": "write", "target": path.Join(ProjectPath, c.name), "resource": c.name, "existed": c.existed}, Content: fmt.Sprintf("%s file successfully: %s", word, c.name)}
	} else {
		if !c.existed {
			return nil, nil, fail("not_found", "The file to edit does not exist.")
		}
		endingsOnly := strings.ReplaceAll(in.Old, "\r", "") == strings.ReplaceAll(in.New, "\r", "")
		if !endingsOnly && (strings.Contains(before, "\r") || strings.Contains(in.Old, "\r") || strings.Contains(in.New, "\r")) {
			return nil, nil, unsupported("Mixed or CRLF edit normalization is unsupported; use an exact complete write.")
		}
		count := strings.Count(before, in.Old)
		if count == 0 {
			return nil, nil, fail("precondition_failed", "The exact old text was not found; fuzzy edits are unsupported.")
		}
		if count > 1 && !in.ReplaceAll {
			return nil, nil, fail("precondition_failed", "The old text is not unique; use replaceAll or add context.")
		}
		n := 1
		if in.ReplaceAll {
			n = -1
		}
		next := strings.Replace(before, in.Old, in.New, n)
		if next == before {
			return nil, nil, fail("precondition_failed", "The edit makes no change.")
		}
		diff, e := fileDiff(c.name, before, strings.TrimPrefix(next, "\ufeff"), "modified")
		if e != nil {
			return nil, nil, e
		}
		if bom && !strings.HasPrefix(next, "\ufeff") {
			next = "\ufeff" + next
		}
		c.after = []byte(next)
		suffix := ""
		if count != 1 {
			suffix = "s"
		}
		result = &ToolResult{Output: map[string]any{"files": []diffInfo{diff}, "replacements": count}, Content: fmt.Sprintf("Edited %s (%d replacement%s)", c.name, count, suffix)}
	}
	if len(c.after) > MaxFileBytes {
		return nil, nil, fail("limit_exceeded", "The resulting file exceeds its limit.")
	}
	return result, func() error { return publish(root, []change{c}) }, nil
}
