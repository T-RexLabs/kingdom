package filetoolhelper

import (
	"fmt"
	"strings"
)

type diffInfo struct {
	File      string `json:"file"`
	Patch     string `json:"patch"`
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
	Status    string `json:"status"`
}
type diffLine struct {
	kind byte
	text string
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.SplitAfter(strings.TrimSuffix(s, "\n"), "\n")
}
func fileDiff(name, before, after, status string) (diffInfo, error) {
	result := diffInfo{File: name, Status: status}
	a, b := splitLines(before), splitLines(after)
	// Include each line ending in equality, as the upstream diff does.
	if strings.HasSuffix(before, "\n") && len(a) > 0 {
		a[len(a)-1] += "\n"
	}
	if strings.HasSuffix(after, "\n") && len(b) > 0 {
		b[len(b)-1] += "\n"
	}
	if (len(a) + 1) > 4000000/(len(b)+1) {
		return result, unsupported("The edit diff is too large; use a complete write.")
	}
	width := len(b) + 1
	dp := make([]uint32, (len(a)+1)*width)
	for i := len(a) - 1; i >= 0; i-- {
		for j := len(b) - 1; j >= 0; j-- {
			if a[i] == b[j] {
				dp[i*width+j] = 1 + dp[(i+1)*width+j+1]
			} else {
				left, right := dp[(i+1)*width+j], dp[i*width+j+1]
				if right > left {
					left = right
				}
				dp[i*width+j] = left
			}
		}
	}
	lines := []diffLine{}
	for i, j := 0, 0; i < len(a) || j < len(b); {
		if i < len(a) && j < len(b) && a[i] == b[j] {
			lines = append(lines, diffLine{' ', a[i]})
			i++
			j++
		} else if i < len(a) && (j == len(b) || dp[(i+1)*width+j] >= dp[i*width+j+1]) {
			lines = append(lines, diffLine{'-', a[i]})
			result.Deletions++
			i++
		} else {
			lines = append(lines, diffLine{'+', b[j]})
			result.Additions++
			j++
		}
	}
	var out strings.Builder
	fmt.Fprintf(&out, "Index: %s\n===================================================================\n--- %s\n+++ %s\n", name, name, name)
	oldLine, newLine := 1, 1
	for start := 0; start < len(lines); {
		change := start
		for change < len(lines) && lines[change].kind == ' ' {
			change++
		}
		if change == len(lines) {
			break
		}
		begin := change - 4
		if begin < start {
			begin = start
		}
		for ; start < begin; start++ {
			oldLine++
			newLine++
		}
		end := change + 1
		for end < len(lines) {
			next := end
			for next < len(lines) && lines[next].kind == ' ' {
				next++
			}
			if next-end > 8 || next == len(lines) {
				end += 4
				if end > len(lines) {
					end = len(lines)
				}
				break
			}
			end = next + 1
		}
		oldCount, newCount := 0, 0
		for _, line := range lines[begin:end] {
			if line.kind != '+' {
				oldCount++
			}
			if line.kind != '-' {
				newCount++
			}
		}
		oldStart, newStart := oldLine, newLine
		if oldCount == 0 {
			oldStart--
		}
		if newCount == 0 {
			newStart--
		}
		fmt.Fprintf(&out, "@@ -%d,%d +%d,%d @@\n", oldStart, oldCount, newStart, newCount)
		for _, line := range lines[begin:end] {
			out.WriteByte(line.kind)
			out.WriteString(line.text)
			if !strings.HasSuffix(line.text, "\n") {
				out.WriteString("\n\\ No newline at end of file\n")
			}
		}
		oldLine += oldCount
		newLine += newCount
		start = end
	}
	result.Patch = out.String()
	return result, nil
}
