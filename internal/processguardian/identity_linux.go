//go:build linux

package processguardian

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const linuxProcessIdentityFilePrefix = ".kingdom-process-instance-"

// The unlinked marker descriptor gives Kingdom processes a random generation
// token that can be inspected by another same-user process through /proc.
var linuxCurrentProcessIdentity struct {
	once  sync.Once
	token string
	file  *os.File
	err   error
}

func processIdentityToken(pid int) (string, error) {
	bootID, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("read linux boot id: %w", err)
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", fmt.Errorf("read process stat: %w", err)
	}
	environment, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
	if err != nil {
		return "", fmt.Errorf("read process environment identity: %w", err)
	}
	identity, err := linuxProcessIdentityToken(strings.TrimSpace(string(bootID)), stat, environment)
	if err != nil {
		return "", err
	}
	fileToken, err := linuxProcessIdentityFileToken(pid)
	if err != nil {
		return "", err
	}
	if fileToken != "" {
		identity += ":fd:" + fileToken
	}
	return identity, nil
}

func linuxProcessIdentityToken(bootID string, stat, environment []byte) (string, error) {
	closing := strings.LastIndex(string(stat), ")")
	if closing < 0 {
		return "", fmt.Errorf("invalid process stat")
	}
	fields := strings.Fields(string(stat[closing+1:]))
	if len(fields) <= 19 {
		return "", fmt.Errorf("incomplete process stat")
	}
	startTicks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || startTicks == 0 {
		return "", fmt.Errorf("invalid process start ticks")
	}
	identity := bootID + ":" + strconv.FormatUint(startTicks, 10)
	instanceToken, err := linuxProcessInstanceToken(environment)
	if err != nil {
		return "", err
	}
	if instanceToken != "" {
		identity += ":instance:" + instanceToken
	}
	return identity, nil
}

func processIdentityNotFound(err error) bool { return errors.Is(err, fs.ErrNotExist) }

func linuxProcessIdentityFileToken(pid int) (string, error) {
	if pid == os.Getpid() {
		linuxCurrentProcessIdentity.once.Do(func() {
			token, err := newRandomHex(16)
			if err != nil {
				linuxCurrentProcessIdentity.err = fmt.Errorf("generate process identity file token: %w", err)
				return
			}
			path := filepath.Join(os.TempDir(), linuxProcessIdentityFilePrefix+token)
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
			if err != nil {
				linuxCurrentProcessIdentity.err = fmt.Errorf("create process identity file: %w", err)
				return
			}
			if err := os.Remove(path); err != nil {
				_ = file.Close()
				linuxCurrentProcessIdentity.err = fmt.Errorf("unlink process identity file: %w", err)
				return
			}
			linuxCurrentProcessIdentity.token = token
			linuxCurrentProcessIdentity.file = file
		})
		return linuxCurrentProcessIdentity.token, linuxCurrentProcessIdentity.err
	}

	fdDirectory := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(fdDirectory)
	if err != nil {
		return "", fmt.Errorf("read process identity descriptors: %w", err)
	}
	token := ""
	for _, entry := range entries {
		link, err := os.Readlink(filepath.Join(fdDirectory, entry.Name()))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("inspect process identity descriptor: %w", err)
		}
		link = strings.TrimSuffix(link, " (deleted)")
		name := filepath.Base(link)
		if !strings.HasPrefix(name, linuxProcessIdentityFilePrefix) {
			continue
		}
		candidate := strings.TrimPrefix(name, linuxProcessIdentityFilePrefix)
		if len(candidate) != 32 {
			return "", fmt.Errorf("invalid process identity file token")
		}
		if token != "" && token != candidate {
			return "", fmt.Errorf("multiple process identity file tokens")
		}
		token = candidate
	}
	return token, nil
}

func configureCommandIdentity(command *Command) error {
	if command == nil {
		return fmt.Errorf("guarded command is required")
	}
	token, err := newRandomHex(16)
	if err != nil {
		return fmt.Errorf("generate guarded process instance token: %w", err)
	}
	command.Env = withProcessIdentityEnvironment(command.Env, token)
	return nil
}

func configureGuardianIdentity(command *exec.Cmd) error {
	if command == nil {
		return fmt.Errorf("guardian command is required")
	}
	token, err := newRandomHex(16)
	if err != nil {
		return fmt.Errorf("generate guardian process instance token: %w", err)
	}
	command.Env = withProcessIdentityEnvironment(os.Environ(), token)
	return nil
}

func linuxProcessInstanceToken(environment []byte) (string, error) {
	name := []byte(processIdentityEnvironmentName + "=")
	token := ""
	for _, entry := range strings.Split(string(environment), "\x00") {
		if !strings.HasPrefix(entry, string(name)) {
			continue
		}
		value := strings.TrimPrefix(entry, string(name))
		if value == "" || token != "" {
			return "", fmt.Errorf("invalid process instance token environment")
		}
		token = value
	}
	return token, nil
}
