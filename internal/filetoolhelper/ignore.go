package filetoolhelper

import (
	"errors"
	"kingdom/internal/securefs"
	"os"
	"path"
	"regexp"
	"strings"
)

type ignoreRule struct {
	base                          string
	pattern                       *regexp.Regexp
	basename, negative, directory bool
}

func ignoreRules(root *securefs.Root, dir string, git bool, parent []ignoreRule) ([]ignoreRule, error) {
	result := append([]ignoreRule(nil), parent...)
	names := []string{".ignore", ".rgignore"}
	if git {
		names = append([]string{".gitignore"}, names...)
	}
	for _, name := range names {
		raw, _, e := text(root, path.Join(dir, name))
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return nil, e
		}
		if len(raw) > 64<<10 {
			return nil, fail("limit_exceeded", "Search ignore file exceeds its limit.")
		}
		for _, line := range strings.Split(string(raw), "\n") {
			line = strings.TrimRight(strings.TrimSuffix(line, "\r"), " ")
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			rule := ignoreRule{base: dir}
			if strings.Contains(line, "\\") {
				return nil, unsupported("Escaped ignore patterns are unsupported.")
			}
			if strings.HasPrefix(line, "!") {
				rule.negative = true
				line = line[1:]
			}
			if line == "" {
				continue
			}
			rule.directory = strings.HasSuffix(line, "/")
			line = strings.TrimSuffix(line, "/")
			anchored := strings.HasPrefix(line, "/")
			line = strings.TrimPrefix(line, "/")
			rule.basename = !anchored && !strings.Contains(line, "/")
			rule.pattern, e = patternRegex(line)
			if e != nil {
				return nil, e
			}
			result = append(result, rule)
			if len(result) > 4096 {
				return nil, fail("limit_exceeded", "Search ignore rule limit exceeded.")
			}
		}
	}
	return result, nil
}
func ignored(name string, directory bool, rules []ignoreRule) bool {
	result := false
	for _, rule := range rules {
		value := name
		if rule.base != "." {
			if !strings.HasPrefix(name, rule.base+"/") {
				continue
			}
			value = strings.TrimPrefix(name, rule.base+"/")
		}
		if rule.directory && !directory {
			continue
		}
		if rule.basename {
			value = path.Base(value)
		}
		if rule.pattern.MatchString(value) {
			result = !rule.negative
		}
	}
	return result
}
