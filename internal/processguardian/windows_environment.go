package processguardian

import (
	"sort"
	"strings"
)

// normalizeWindowsEnvironment sorts environment entries case-insensitively and
// keeps each per-drive current-directory entry as a distinct variable. Windows
// represents those entries as `=C:=C:\path`, so splitting at the first `=`
// would collapse all drives under the same empty key.
func normalizeWindowsEnvironment(environment []string) []string {
	entries := make(map[string]string, len(environment))
	for _, entry := range environment {
		name := windowsEnvironmentEntryName(entry)
		entries[name] = entry
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make([]string, 0, len(keys))
	for _, key := range keys {
		ordered = append(ordered, entries[key])
	}
	return ordered
}

func windowsEnvironmentEntryName(entry string) string {
	if strings.HasPrefix(entry, "=") {
		if separator := strings.IndexByte(entry[1:], '='); separator >= 0 {
			return strings.ToUpper(entry[:separator+1])
		}
		return strings.ToUpper(entry)
	}
	name, _, _ := strings.Cut(entry, "=")
	return strings.ToUpper(name)
}
