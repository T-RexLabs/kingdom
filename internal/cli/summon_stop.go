package cli

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"kingdom/internal/securefs"
)

const (
	defaultSummonStopRequestFileName = "summon.stop"
	summonStopRequestPollInterval    = 50 * time.Millisecond
	maxSummonStopRequestBytes        = 256
	maxSummonPIDRecordBytes          = 4 << 10
)

func summonStopRequestPath(kingdomHome string) string {
	return filepath.Join(kingdomHome, "run", defaultSummonStopRequestFileName)
}

func generateSummonStopControlToken() (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate summon stop control token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func writeSummonStopRequest(path, processIdentityToken string) error {
	if strings.TrimSpace(path) == "" || strings.TrimSpace(processIdentityToken) == "" {
		return fmt.Errorf("summon stop request path and process identity are required")
	}
	root, err := securefs.OpenRoot(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open summon stop request directory: %w", err)
	}
	defer root.Close()
	if err := root.ReplaceRegular(filepath.Base(path), []byte(processIdentityToken), 0o600, 0o700, time.Now().UTC()); err != nil {
		return fmt.Errorf("publish summon stop request: %w", err)
	}
	return nil
}

func removeSummonPIDRecordIfIdentity(path string, expected summonPIDRecord) error {
	if expected.PID <= 0 || strings.TrimSpace(expected.ProcessIdentityToken) == "" || strings.TrimSpace(expected.StopControlToken) == "" {
		return nil
	}
	root, err := securefs.OpenRoot(filepath.Dir(path))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open summon pid directory for cleanup: %w", err)
	}
	defer root.Close()
	name := filepath.Base(path)
	raw, identity, err := root.ReadRegularIdentified(name, maxSummonPIDRecordBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read summon pid record for cleanup: %w", err)
	}
	var current summonPIDRecord
	if err := json.Unmarshal(raw, &current); err != nil || current.PID != expected.PID || current.ProcessIdentityToken != expected.ProcessIdentityToken || current.StopControlToken != expected.StopControlToken {
		return nil
	}
	err = root.RemoveRegularIfIdentity(name, identity)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, securefs.ErrIdentityMismatch) {
		return nil
	}
	return err
}

func cleanupSummonProcessGeneration(pidPath string, pid int, processIdentityToken, stopControlToken string) error {
	return errors.Join(
		removeSummonPIDRecordIfIdentity(pidPath, summonPIDRecord{PID: pid, ProcessIdentityToken: processIdentityToken, StopControlToken: stopControlToken}),
		unregisterSummonInstance(pid, processIdentityToken, stopControlToken),
	)
}

func consumeSummonStopRequest(path, processIdentityToken string) (bool, error) {
	if strings.TrimSpace(path) == "" || strings.TrimSpace(processIdentityToken) == "" {
		return false, fmt.Errorf("summon stop request path and process identity are required")
	}
	root, err := securefs.OpenRoot(filepath.Dir(path))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	name := filepath.Base(path)
	raw, identity, err := root.ReadRegularIdentified(name, maxSummonStopRequestBytes)
	if errors.Is(err, fs.ErrNotExist) {
		_ = root.Close()
		return false, nil
	}
	if err != nil {
		_ = root.Close()
		return false, err
	}
	if string(raw) != processIdentityToken {
		_ = root.Close()
		return false, nil
	}
	removeErr := root.RemoveRegularIfIdentity(name, identity)
	closeErr := root.Close()
	if errors.Is(removeErr, fs.ErrNotExist) || errors.Is(removeErr, securefs.ErrIdentityMismatch) {
		removeErr = nil
		return false, errors.Join(removeErr, closeErr)
	}
	if removeErr != nil {
		return false, errors.Join(removeErr, closeErr)
	}
	return true, closeErr
}

// listenForSummonStop watches the protected stop-request file and forwards a
// shutdown signal only when the request names this process generation. A stale
// request for a reused PID therefore cannot stop its replacement.
func listenForSummonStop(signals chan<- os.Signal, path, processIdentityToken string) (func(), error) {
	if signals == nil || strings.TrimSpace(path) == "" || strings.TrimSpace(processIdentityToken) == "" {
		return nil, fmt.Errorf("summon stop signal channel, request path, and process identity are required")
	}
	platformSignals := summonSignals()
	if len(platformSignals) == 0 {
		return nil, fmt.Errorf("summon stop signal is unavailable on this platform")
	}
	stop := make(chan struct{})
	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		ticker := time.NewTicker(summonStopRequestPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
			received, err := consumeSummonStopRequest(path, processIdentityToken)
			if err != nil || !received {
				continue
			}
			select {
			case signals <- platformSignals[0]:
			case <-stop:
				return
			}
		}
	}()
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			close(stop)
			<-waitDone
		})
	}
	return cleanup, nil
}

func watchSummonStartupStop(signals <-chan os.Signal, cancel context.CancelFunc) (<-chan struct{}, func() bool) {
	requested := make(chan struct{}, 1)
	stop := make(chan struct{})
	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		select {
		case <-stop:
			return
		case <-signals:
			select {
			case requested <- struct{}{}:
			default:
			}
			cancel()
		}
	}()
	var finishOnce sync.Once
	stopped := false
	finish := func() bool {
		finishOnce.Do(func() {
			close(stop)
			<-waitDone
			select {
			case <-requested:
				stopped = true
			default:
			}
			select {
			case <-signals:
				stopped = true
			default:
			}
		})
		return stopped
	}
	return requested, finish
}
