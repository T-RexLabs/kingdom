package filetoolhelper

import (
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"unicode/utf16"

	"kingdom/internal/securefs"
)

func page(in input) (int, int) {
	offset, limit := 1, 2000
	if in.Offset != nil && *in.Offset > 0 {
		offset = *in.Offset
	}
	if in.Limit != nil && *in.Limit > 0 && *in.Limit < limit {
		limit = *in.Limit
	}
	return offset, limit
}
func clampLine(s string) string {
	r := utf16.Encode([]rune(s))
	if len(r) <= 2000 {
		return s
	}
	return string(utf16.Decode(r[:2000])) + "... (line truncated to 2000 chars)"
}
func mimeFor(name string) string {
	switch strings.ToLower(path.Ext(name)) {
	case ".json":
		return "application/json"
	case ".html":
		return "text/html"
	case ".css":
		return "text/css"
	case ".js", ".mjs":
		return "text/javascript"
	case ".md":
		return "text/markdown"
	case ".xml":
		return "application/xml"
	case ".csv":
		return "text/csv"
	}
	return "text/plain"
}
func read(root *securefs.Root, in input) (*ToolResult, error) {
	name, e := relative(in.Path)
	if e != nil {
		return nil, e
	}
	offset, limit := page(in)
	directory := name == "."
	if !directory {
		entry, e := root.ReadEntry(name)
		if e != nil {
			return nil, e
		}
		directory = entry.Type == securefs.EntryTypeDirectory
		if entry.Type != securefs.EntryTypeRegular && !directory {
			return nil, unsupported("Only regular files and directories can be read.")
		}
	}
	if directory {
		entries, e := root.ReadDirBounded(name, MaxEntries)
		if e != nil {
			return nil, e
		}
		type item struct {
			Path string `json:"path"`
			Type string `json:"type"`
		}
		visible := []item{}
		for _, entry := range entries {
			if _, e := relative(path.Join(name, entry.Name)); e != nil {
				continue
			}
			kind := string(entry.Type)
			if kind == "regular" {
				kind = "file"
			}
			if kind != "file" && kind != "directory" && kind != "symlink" {
				continue
			}
			p := entry.Name
			if kind == "directory" {
				p += "/"
			}
			visible = append(visible, item{p, kind})
		}
		sort.Slice(visible, func(i, j int) bool {
			if (visible[i].Type == "directory") != (visible[j].Type == "directory") {
				return visible[i].Type == "directory"
			}
			return visible[i].Path < visible[j].Path
		})
		start := offset - 1
		if start > len(visible) {
			start = len(visible)
		}
		end := start + limit
		if end > len(visible) {
			end = len(visible)
		}
		selected := visible[start:end]
		truncated := end < len(visible)
		out := map[string]any{"type": "list-page", "entries": selected, "truncated": truncated}
		content := fmt.Sprintf("Read directory %s, 0 entries", in.Path)
		if len(selected) > 0 {
			content = fmt.Sprintf("Read directory %s, entries %d-%d", in.Path, offset, offset+len(selected)-1)
		}
		for _, entry := range selected {
			content += "\n" + entry.Path
		}
		if truncated {
			next := offset + len(selected)
			out["next"] = next
			content += fmt.Sprintf("\n[Output truncated. Continue reading with offset: %d]", next)
		}
		return &ToolResult{Output: out, Content: content, Metadata: map[string]any{"truncated": truncated}}, nil
	}
	raw, _, e := text(root, name)
	if e != nil {
		return nil, e
	}
	source := strings.TrimPrefix(string(raw), "\ufeff")
	var out map[string]any
	var lines []string
	truncated := false
	if len(raw) <= 50*1024 && in.Offset == nil && in.Limit == nil {
		all := strings.Split(source, "\n")
		for i := range all {
			all[i] = clampLine(all[i])
		}
		source = strings.Join(all, "\n")
		uri := (&url.URL{Scheme: "file", Path: path.Join(ProjectPath, name)}).String()
		out = map[string]any{"type": "file", "uri": uri, "name": path.Base(name), "content": source, "encoding": "utf8", "mime": mimeFor(name)}
		if source != "" {
			lines = strings.Split(strings.TrimSuffix(source, "\n"), "\n")
		}
	} else {
		all := strings.Split(source, "\n")
		if all[len(all)-1] == "" {
			all = all[:len(all)-1]
		}
		if offset > len(all) && offset != 1 {
			return nil, fail("offset_out_of_range", "The read offset is beyond the file.")
		}
		size := 0
		for i := offset - 1; i < len(all); i++ {
			line := clampLine(strings.TrimSuffix(all[i], "\r"))
			extra := len(line)
			if len(lines) > 0 {
				extra++
			}
			if len(lines) >= limit || size+extra > 50*1024 {
				truncated = true
				break
			}
			lines = append(lines, line)
			size += extra
		}
		out = map[string]any{"type": "text-page", "content": strings.Join(lines, "\n"), "mime": mimeFor(name), "offset": offset, "truncated": truncated}
		if truncated {
			out["next"] = offset + len(lines)
		}
	}
	content := fmt.Sprintf("Read file %s, 0 lines", in.Path)
	if len(lines) > 0 {
		content = fmt.Sprintf("Read file %s, lines %d-%d", in.Path, offset, offset+len(lines)-1)
	}
	for i, line := range lines {
		content += fmt.Sprintf("\n%d: %s", offset+i, line)
	}
	if truncated {
		content += fmt.Sprintf("\n[Output truncated. Continue reading with offset: %d]", offset+len(lines))
	}
	return &ToolResult{Output: out, Content: content, Metadata: map[string]any{"truncated": truncated}}, nil
}
