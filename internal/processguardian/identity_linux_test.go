//go:build linux

package processguardian

import (
	"os"
	"strings"
	"testing"
)

func TestLinuxProcessIdentityDistinguishesSameTickGenerations(t *testing.T) {
	statFields := []string{"S", "1", "1", "1", "0", "-1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "20", "0", "1", "0", "42"}
	stat := []byte("123 (provider) " + strings.Join(statFields, " "))
	first, err := linuxProcessIdentityToken("boot-id", stat, []byte(processIdentityEnvironmentName+"=generation-one\x00"))
	if err != nil {
		t.Fatalf("first generation token: %v", err)
	}
	second, err := linuxProcessIdentityToken("boot-id", stat, []byte(processIdentityEnvironmentName+"=generation-two\x00"))
	if err != nil {
		t.Fatalf("second generation token: %v", err)
	}
	if first == second {
		t.Fatalf("same-tick process generations share identity %q", first)
	}
}

func TestConfigureCommandIdentityReplacesCallerToken(t *testing.T) {
	command := Command{Env: []string{"PATH=/usr/bin", processIdentityEnvironmentName + "=caller-value"}}
	if err := configureCommandIdentity(&command); err != nil {
		t.Fatalf("configure command identity: %v", err)
	}
	if token := processIdentityEnvironmentValue(command.Env); len(token) != 32 || token == "caller-value" {
		t.Fatalf("configured process identity token=%q, want a generated 32-character token", token)
	}
	if len(command.Env) != 2 || command.Env[0] != "PATH=/usr/bin" {
		t.Fatalf("command environment after identity setup=%q", command.Env)
	}
}

func TestLinuxCurrentProcessIdentityIncludesStableFileMarker(t *testing.T) {
	first, err := ProcessIdentityToken(os.Getpid())
	if err != nil {
		t.Fatalf("read current process identity: %v", err)
	}
	second, err := ProcessIdentityToken(os.Getpid())
	if err != nil {
		t.Fatalf("read current process identity again: %v", err)
	}
	if first != second || !strings.Contains(first, ":fd:") {
		t.Fatalf("current process identity changed or lacks file marker: first=%q second=%q", first, second)
	}
}
