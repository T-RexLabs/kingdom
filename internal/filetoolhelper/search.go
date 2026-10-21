package filetoolhelper

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"kingdom/internal/securefs"
)

type searchEntry struct {
	Path string `json:"path"`
	Type string `json:"type"`
}
type submatch struct {
	Text  string `json:"text"`
	Start int    `json:"start"`
	End   int    `json:"end"`
}
type searchMatch struct {
	Entry      searchEntry `json:"entry"`
	Line       int         `json:"line"`
	Offset     int         `json:"offset"`
	Text       string      `json:"text"`
	Submatches []submatch  `json:"submatches"`
}

func patternRegex(pattern string) (*regexp.Regexp, error) {
	if len(pattern) > 4096 || strings.ContainsAny(pattern, "\\\x00") || strings.Contains(pattern, "..") || strings.HasPrefix(pattern, "/") {
		return nil, unsupported("The glob pattern is unsupported.")
	}
	var out strings.Builder
	out.WriteString("^")
	depth := 0
	for i := 0; i < len(pattern); i++ {
		switch c := pattern[i]; c {
		case '*':
			if i+1 < len(pattern) && pattern[i+1] == '*' {
				i++
				if i+1 < len(pattern) && pattern[i+1] == '/' {
					i++
					out.WriteString("(?:.*/)?")
				} else {
					out.WriteString(".*")
				}
			} else {
				out.WriteString("[^/]*")
			}
		case '?':
			out.WriteString("[^/]")
		case '{':
			if depth != 0 {
				return nil, unsupported("Nested glob alternatives are unsupported.")
			}
			depth++
			out.WriteString("(?:")
		case '}':
			if depth != 1 {
				return nil, unsupported("Invalid glob alternative.")
			}
			depth--
			out.WriteString(")")
		case ',':
			if depth == 1 {
				out.WriteByte('|')
			} else {
				out.WriteByte(',')
			}
		case '[', ']':
			return nil, unsupported("Glob character classes are unsupported.")
		default:
			if c >= utf8.RuneSelf {
				r, width := utf8.DecodeRuneInString(pattern[i:])
				out.WriteString(regexp.QuoteMeta(string(r)))
				i += width - 1
			} else {
				out.WriteString(regexp.QuoteMeta(string(c)))
			}
		}
	}
	if depth != 0 {
		return nil, unsupported("Invalid glob alternative.")
	}
	out.WriteString("$")
	r, e := regexp.Compile(out.String())
	if e != nil {
		return nil, unsupported("Invalid glob pattern.")
	}
	return r, nil
}
func scan(ctx context.Context, root *securefs.Root, base string, hidden bool, positiveGlob bool) ([]string, error) {
	files := []string{}
	count := 0
	git := false
	if _, e := root.ReadEntry(".git"); e == nil {
		git = true
	}
	var walk func(string, int, []ignoreRule) error
	walk = func(dir string, depth int, inherited []ignoreRule) error {
		rules, e := ignoreRules(root, dir, git, inherited)
		if e != nil {
			return e
		}
		if e := ctx.Err(); e != nil {
			return e
		}
		if depth > 64 {
			return fail("limit_exceeded", "Search depth exceeds its limit.")
		}
		entries, e := root.ReadDirBounded(dir, MaxEntries-count)
		if e != nil {
			return e
		}
		count += len(entries)
		if count > MaxEntries {
			return fail("limit_exceeded", "Search entry limit exceeded.")
		}
		for _, entry := range entries {
			if !hidden && strings.HasPrefix(entry.Name, ".") {
				continue
			}
			name := path.Join(dir, entry.Name)
			if _, e := relative(name); e != nil {
				continue
			}
			if ignored(name, entry.Type == securefs.EntryTypeDirectory, rules) && (entry.Type == securefs.EntryTypeDirectory || !positiveGlob) {
				continue
			}
			switch entry.Type {
			case securefs.EntryTypeDirectory:
				if e := walk(name, depth+1, rules); e != nil {
					return e
				}
			case securefs.EntryTypeRegular:
				files = append(files, name)
			}
		}
		return nil
	}
	if base != "." {
		entry, e := root.ReadEntry(base)
		if e != nil {
			return nil, e
		}
		if entry.Type == securefs.EntryTypeRegular {
			return []string{base}, nil
		}
		if entry.Type != securefs.EntryTypeDirectory {
			return nil, unsupported("Search does not follow symbolic links.")
		}
	}
	ancestorRules := []ignoreRule{}
	if base != "." {
		dirs := []string{"."}
		current := "."
		parts := strings.Split(base, "/")
		for _, component := range parts[:len(parts)-1] {
			current = path.Join(current, component)
			dirs = append(dirs, current)
		}
		for _, dir := range dirs {
			var err error
			ancestorRules, err = ignoreRules(root, dir, git, ancestorRules)
			if err != nil {
				return nil, err
			}
		}
	}
	if e := walk(base, 0, ancestorRules); e != nil {
		return nil, e
	}
	sort.Strings(files)
	return files, nil
}
func search(ctx context.Context, root *securefs.Root, tool string, in input) (*ToolResult, error) {
	if tool == "glob" && (in.Path == "undefined" || in.Path == "null") {
		in.Path = ""
	}
	base, e := relative(in.Path)
	if e != nil {
		return nil, e
	}
	if tool == "glob" && base != "." {
		entry, err := root.ReadEntry(base)
		if err != nil {
			return nil, err
		}
		if entry.Type != securefs.EntryTypeDirectory {
			return nil, unsupported("Glob requires a directory path.")
		}
	}
	limit := 100
	if in.Limit != nil {
		if *in.Limit < 1 {
			return nil, fail("invalid_input", "Search limit must be positive.")
		}
		limit = *in.Limit
	}
	var glob, matcher *regexp.Regexp
	if tool == "glob" {
		glob, e = patternRegex(in.Pattern)
	} else {
		if in.Include != "" {
			glob, e = patternRegex(in.Include)
		}
		pattern := in.Pattern
		if !in.Literal {
			for _, class := range []string{`\w`, `\W`, `\b`, `\B`, `\d`, `\D`, `\s`, `\S`} {
				if strings.Contains(pattern, class) {
					return nil, unsupported("Unicode shorthand regex classes are unsupported; use explicit character classes or literal matching.")
				}
			}
		}
		if in.Literal {
			pattern = regexp.QuoteMeta(pattern)
		}
		if in.CaseSensitive != nil && !*in.CaseSensitive {
			pattern = "(?i)" + pattern
		}
		if len(pattern) > 16384 {
			return nil, unsupported("The search pattern exceeds its limit.")
		}
		matcher, e = regexp.Compile(pattern)
	}
	if e != nil {
		return nil, unsupported("The search pattern is unsupported or invalid.")
	}
	files, e := scan(ctx, root, base, in.Hidden || tool == "grep", glob != nil)
	if e != nil {
		return nil, e
	}
	entries := []searchEntry{}
	matches := []searchMatch{}
	truncated := false
	totalBytes := int64(0)
	for _, name := range files {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		matchName := strings.TrimPrefix(name, base+"/")
		if base == "." {
			matchName = name
		}
		if glob != nil && !glob.MatchString(matchName) && !(tool == "grep" && !strings.Contains(in.Include, "/") && glob.MatchString(path.Base(name))) {
			continue
		}
		entry := searchEntry{name, "file"}
		if tool == "glob" {
			if len(entries) == limit {
				truncated = true
				break
			}
			entries = append(entries, entry)
			continue
		}
		raw, _, err := text(root, name)
		if err != nil {
			var known *operationError
			if errors.As(err, &known) && known.code == "unsupported" {
				continue
			}
			return nil, err
		}
		totalBytes += int64(len(raw))
		if totalBytes > 64<<20 {
			return nil, fail("limit_exceeded", "Search byte limit exceeded.")
		}
		offset := 0
		lines := strings.SplitAfter(string(raw), "\n")
		for index, line := range lines {
			if index == len(lines)-1 && line == "" {
				break
			}
			locations := matcher.FindAllStringIndex(line, 100)
			if len(locations) > 0 {
				if len(matches) == limit {
					truncated = true
					break
				}
				clean := line
				if len([]rune(clean)) > 2000 {
					clean = string([]rune(clean)[:2000]) + "..."
				}
				if len(clean) > 16*1024 {
					return nil, fail("limit_exceeded", "A matching line exceeds its output limit.")
				}
				subs := []submatch{}
				for _, loc := range locations {
					subs = append(subs, submatch{line[loc[0]:loc[1]], loc[0], loc[1]})
				}
				matches = append(matches, searchMatch{entry, index + 1, offset, clean, subs})
			}
			offset += len(line)
		}
		if truncated {
			break
		}
	}
	if tool == "glob" {
		content := "No files found"
		if len(entries) > 0 {
			lines := []string{}
			for _, v := range entries {
				lines = append(lines, path.Join(ProjectPath, v.Path))
			}
			content = strings.Join(lines, "\n")
		}
		if truncated {
			content += fmt.Sprintf("\n\n(Results are truncated: showing first %d results. Consider using a more specific path or pattern.)", len(entries))
		}
		return &ToolResult{Output: entries, Content: content, Metadata: map[string]any{"count": len(entries), "truncated": truncated}}, nil
	}
	content := "No matches found"
	if len(matches) > 0 {
		content = fmt.Sprintf("Found %d matches", len(matches))
		last := ""
		for _, m := range matches {
			if m.Entry.Path != last {
				if last != "" {
					content += "\n"
				}
				last = m.Entry.Path
				content += "\n" + path.Join(ProjectPath, last) + ":"
			}
			content += fmt.Sprintf("\n  Line %d: %s", m.Line, m.Text)
		}
	}
	if truncated {
		content += fmt.Sprintf("\n\n(Results are truncated: showing first %d results. Consider using a more specific path or pattern.)", len(matches))
	}
	return &ToolResult{Output: matches, Content: content, Metadata: map[string]any{"matches": len(matches), "truncated": truncated}}, nil
}
