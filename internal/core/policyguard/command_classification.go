package policyguard

import "strings"

func classifyForbiddenCommand(command string) (string, bool) {
	normalized := normalizeCommand(command)
	if normalized == "" {
		return "", false
	}

	if strings.HasPrefix(normalized, "sudo ") || normalized == "sudo" || strings.HasPrefix(normalized, "su ") || normalized == "su" {
		return "privilege_escalation", true
	}

	if strings.HasPrefix(normalized, "git reset --hard") || strings.HasPrefix(normalized, "git checkout --") || strings.HasPrefix(normalized, "git clean -fd") || strings.Contains(normalized, "git push --force") {
		return "git_destructive", true
	}

	if strings.HasPrefix(normalized, "sqlite3 ") || strings.Contains(normalized, " update ") || strings.Contains(normalized, " insert ") || strings.Contains(normalized, " delete ") {
		if strings.Contains(normalized, "sqlite") || strings.Contains(normalized, "psql") || strings.Contains(normalized, "mysql") {
			return "direct_state_bypass", true
		}
	}

	if strings.HasPrefix(normalized, "brew ") || strings.HasPrefix(normalized, "apt ") || strings.HasPrefix(normalized, "apt-get ") || strings.HasPrefix(normalized, "yum ") || strings.HasPrefix(normalized, "dnf ") || strings.HasPrefix(normalized, "launchctl ") {
		return "host_system_mutation", true
	}

	return "", false
}

func inferCommandClass(command string) (string, bool) {
	normalized := normalizeCommand(command)
	if normalized == "" {
		return "", false
	}

	switch {
	case strings.HasPrefix(normalized, "git "):
		return "git_safe", true
	case strings.HasPrefix(normalized, "go "):
		if strings.HasPrefix(normalized, "go test") {
			return "test_runner", true
		}
		return "go_toolchain", true
	case strings.HasPrefix(normalized, "npm "), strings.HasPrefix(normalized, "pnpm "), strings.HasPrefix(normalized, "node "), strings.HasPrefix(normalized, "npx "):
		if strings.Contains(normalized, " test") {
			return "test_runner", true
		}
		return "node_toolchain", true
	case strings.HasPrefix(normalized, "pytest"), strings.HasPrefix(normalized, "go test"):
		return "test_runner", true
	case strings.HasPrefix(normalized, "trivy "), strings.HasPrefix(normalized, "gitleaks "):
		return "security_scanner", true
	case strings.HasPrefix(normalized, "toxiproxy "):
		return "chaos_injector", true
	default:
		return "", false
	}
}

func normalizeCommand(command string) string {
	return strings.ToLower(strings.Join(strings.Fields(command), " "))
}
