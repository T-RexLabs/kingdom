package filetoolhelper

import (
	"fmt"
	"kingdom/internal/securefs"
	"strings"
)

type patchChunk struct {
	old, next []string
	context   string
	eof       bool
}
type patchHunk struct {
	kind, name, move, content string
	chunks                    []patchChunk
}

func parsePatch(source string) ([]patchHunk, error) {
	lines := strings.Split(strings.TrimSpace(source), "\n")
	if len(lines) < 3 || strings.TrimSpace(lines[0]) != "*** Begin Patch" || strings.TrimSpace(lines[len(lines)-1]) != "*** End Patch" {
		return nil, unsupported("Patch requires exact Begin Patch and End Patch boundaries.")
	}
	for i := range lines {
		lines[i] = strings.TrimSuffix(lines[i], "\r")
	}
	hunks := []patchHunk{}
	for i := 1; i < len(lines)-1; {
		line := lines[i]
		h := patchHunk{}
		for _, kind := range []string{"Add", "Delete", "Update"} {
			prefix := "*** " + kind + " File: "
			if strings.HasPrefix(line, prefix) {
				h.kind = strings.ToLower(kind)
				h.name = strings.TrimSpace(strings.TrimPrefix(line, prefix))
				break
			}
		}
		if h.kind == "" || h.name == "" {
			return nil, unsupported("Unsupported patch header.")
		}
		i++
		if h.kind == "update" && i < len(lines)-1 && strings.HasPrefix(lines[i], "*** Move to: ") {
			h.move = strings.TrimSpace(strings.TrimPrefix(lines[i], "*** Move to: "))
			i++
		}
		body := []string{}
		for i < len(lines)-1 && !strings.HasPrefix(lines[i], "*** Add File: ") && !strings.HasPrefix(lines[i], "*** Delete File: ") && !strings.HasPrefix(lines[i], "*** Update File: ") {
			body = append(body, lines[i])
			i++
		}
		switch h.kind {
		case "delete":
			if len(body) > 0 {
				return nil, fail("invalid_patch", "A delete patch cannot contain body lines.")
			}
		case "add":
			content := []string{}
			for _, line := range body {
				if !strings.HasPrefix(line, "+") {
					return nil, fail("invalid_patch", "Every added file line must start with +.")
				}
				content = append(content, line[1:])
			}
			h.content = strings.Join(content, "\n")
			if h.content != "" && !strings.HasSuffix(h.content, "\n") {
				h.content += "\n"
			}
		case "update":
			for _, line := range body {
				if line == "@@" || strings.HasPrefix(line, "@@ ") {
					if len(h.chunks) > 0 && len(h.chunks[len(h.chunks)-1].old) == 0 && len(h.chunks[len(h.chunks)-1].next) == 0 {
						return nil, fail("invalid_patch", "Empty patch chunk.")
					}
					context := ""
					if line != "@@" {
						context = line[3:]
					}
					h.chunks = append(h.chunks, patchChunk{context: context})
					continue
				}
				if len(h.chunks) == 0 {
					h.chunks = append(h.chunks, patchChunk{})
				}
				c := &h.chunks[len(h.chunks)-1]
				if line == "*** End of File" {
					c.eof = true
					continue
				}
				if c.eof {
					return nil, unsupported("Patch content after End of File is unsupported.")
				}
				switch {
				case line == "":
					c.old = append(c.old, "")
					c.next = append(c.next, "")
				case strings.HasPrefix(line, " "):
					c.old = append(c.old, line[1:])
					c.next = append(c.next, line[1:])
				case strings.HasPrefix(line, "-"):
					c.old = append(c.old, line[1:])
				case strings.HasPrefix(line, "+"):
					c.next = append(c.next, line[1:])
				default:
					return nil, fail("invalid_patch", "Unsupported patch chunk line.")
				}
			}
			if len(h.chunks) == 0 {
				return nil, fail("invalid_patch", "An update patch must contain a chunk.")
			}
		}
		hunks = append(hunks, h)
		if len(hunks) > 128 {
			return nil, fail("limit_exceeded", "Patch file limit exceeded.")
		}
	}
	if len(hunks) == 0 {
		return nil, fail("invalid_patch", "The patch is empty.")
	}
	return hunks, nil
}
func seekExact(lines, pattern []string, start int, eof bool) int {
	if len(pattern) == 0 {
		return -1
	}
	end := len(lines) - len(pattern)
	if eof {
		start = end
	}
	if start < 0 {
		return -1
	}
	for i := start; i <= end; i++ {
		same := true
		for j := range pattern {
			if lines[i+j] != pattern[j] {
				same = false
				break
			}
		}
		if same {
			return i
		}
		if eof {
			break
		}
	}
	return -1
}
func derive(source string, chunks []patchChunk) (string, error) {
	if strings.Contains(source, "\r") {
		return "", unsupported("CRLF patch normalization is unsupported; use a complete write.")
	}
	bom := strings.HasPrefix(source, "\ufeff")
	source = strings.TrimPrefix(source, "\ufeff")
	lines := strings.Split(source, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	type replacement struct {
		start, count int
		next         []string
	}
	replacements := []replacement{}
	cursor := 0
	for _, chunk := range chunks {
		if chunk.context != "" {
			i := seekExact(lines, []string{chunk.context}, cursor, false)
			if i < 0 {
				return "", fail("precondition_failed", "Exact patch context was not found; fuzzy matching is unsupported.")
			}
			cursor = i + 1
		}
		if len(chunk.old) == 0 {
			if len(chunk.next) == 0 {
				return "", fail("invalid_patch", "Empty patch chunk.")
			}
			replacements = append(replacements, replacement{len(lines), 0, chunk.next})
			continue
		}
		old, next := chunk.old, chunk.next
		found := seekExact(lines, old, cursor, chunk.eof)
		if found < 0 && old[len(old)-1] == "" {
			old = old[:len(old)-1]
			if len(next) > 0 && next[len(next)-1] == "" {
				next = next[:len(next)-1]
			}
			found = seekExact(lines, old, cursor, chunk.eof)
		}
		if found < cursor {
			return "", fail("precondition_failed", "Exact patch lines were not found; fuzzy matching is unsupported.")
		}
		replacements = append(replacements, replacement{found, len(old), next})
		cursor = found + len(old)
	}
	for i := len(replacements) - 1; i >= 0; i-- {
		r := replacements[i]
		tail := append([]string(nil), lines[r.start+r.count:]...)
		lines = append(lines[:r.start], r.next...)
		lines = append(lines, tail...)
	}
	if len(lines) == 0 || lines[len(lines)-1] != "" {
		lines = append(lines, "")
	}
	next := strings.Join(lines, "\n")
	if bom && !strings.HasPrefix(next, "\ufeff") {
		next = "\ufeff" + next
	}
	return next, nil
}
func patch(root *securefs.Root, source string) (*ToolResult, func() error, error) {
	hunks, e := parsePatch(source)
	if e != nil {
		return nil, nil, e
	}
	changes := []change{}
	files := []diffInfo{}
	applied := []map[string]string{}
	seen := map[string]bool{}
	totalBytes, totalDiff := 0, 0
	for _, h := range hunks {
		c, e := prepare(root, h.name)
		if e != nil {
			return nil, nil, e
		}
		if seen[c.name] {
			return nil, nil, unsupported("Multiple patch operations on one path are unsupported.")
		}
		seen[c.name] = true
		status := "modified"
		destination := c.name
		switch h.kind {
		case "add":
			if c.existed {
				return nil, nil, fail("precondition_failed", "An added patch file already exists.")
			}
			c.after = []byte(h.content)
			status = "added"
		case "delete":
			if !c.existed {
				return nil, nil, fail("not_found", "A deleted patch file does not exist.")
			}
			c.remove = true
			status = "deleted"
		case "update":
			if !c.existed {
				return nil, nil, fail("not_found", "An updated patch file does not exist.")
			}
			next, e := derive(string(c.before), h.chunks)
			if e != nil {
				return nil, nil, e
			}
			c.after = []byte(next)
		}
		totalBytes += len(c.before) + len(c.after)
		if totalBytes > 32<<20 {
			return nil, nil, fail("limit_exceeded", "Patch memory limit exceeded.")
		}
		if len(c.after) > MaxFileBytes {
			return nil, nil, fail("limit_exceeded", "The patched file exceeds its limit.")
		}
		diff, e := fileDiff(c.name, strings.TrimPrefix(string(c.before), "\ufeff"), strings.TrimPrefix(string(c.after), "\ufeff"), status)
		if e != nil {
			return nil, nil, e
		}
		totalDiff += len(diff.Patch)
		if totalDiff > MaxResultBytes/2 {
			return nil, nil, fail("limit_exceeded", "Patch diff output exceeds its limit.")
		}
		if h.move != "" {
			target, e := prepare(root, h.move)
			if e != nil {
				return nil, nil, e
			}
			if target.existed || seen[target.name] {
				return nil, nil, fail("precondition_failed", "Patch move destination already exists or overlaps another change.")
			}
			seen[target.name] = true
			destination = target.name
			target.after = c.after
			c.after = nil
			c.remove = true
			changes = append(changes, target)
		}
		diff.File = destination
		files = append(files, diff)
		changes = append(changes, c)
		applied = append(applied, map[string]string{"type": h.kind, "resource": destination, "target": ProjectPath + "/" + destination})
	}
	content := "Success. Updated the following files:"
	for _, item := range applied {
		code := "M"
		if item["type"] == "add" {
			code = "A"
		} else if item["type"] == "delete" {
			code = "D"
		}
		content += fmt.Sprintf("\n%s %s", code, item["resource"])
	}
	return &ToolResult{Output: map[string]any{"applied": applied, "files": files}, Content: content}, func() error { return publish(root, changes) }, nil
}
