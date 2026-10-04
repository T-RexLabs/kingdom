//go:build linux

package processguardian

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type linuxProcessStat struct {
	pid        int
	group      int
	state      byte
	startTicks uint64
	threads    int
}

func (s linuxProcessStat) exited() bool {
	return s.threads == 1 && (s.state == 'Z' || s.state == 'X' || s.state == 'x')
}

func readLinuxProcessStat(procRoot string, pid int) (linuxProcessStat, error) {
	process, err := os.OpenRoot(filepath.Join(procRoot, strconv.Itoa(pid)))
	if err != nil {
		return linuxProcessStat{}, err
	}
	defer process.Close()
	raw, err := process.ReadFile("stat")
	if err != nil {
		return linuxProcessStat{}, err
	}
	return parseLinuxProcessStat(raw, pid)
}

func parseLinuxProcessStat(raw []byte, pid int) (linuxProcessStat, error) {
	opening, closing := strings.IndexByte(string(raw), '('), strings.LastIndexByte(string(raw), ')')
	if opening < 0 || closing <= opening {
		return linuxProcessStat{}, fmt.Errorf("invalid Linux process stat for %d", pid)
	}
	actualPID, pidErr := strconv.Atoi(strings.TrimSpace(string(raw[:opening])))
	fields := strings.Fields(string(raw[closing+1:]))
	if pidErr != nil || actualPID != pid || len(fields) < 20 || len(fields[0]) != 1 {
		return linuxProcessStat{}, fmt.Errorf("incomplete Linux process stat for %d", pid)
	}
	group, groupErr := strconv.Atoi(fields[2])
	threads, threadsErr := strconv.Atoi(fields[17])
	ticks, ticksErr := strconv.ParseUint(fields[19], 10, 64)
	if groupErr != nil || group < 0 || ticksErr != nil || threadsErr != nil || threads <= 0 {
		return linuxProcessStat{}, fmt.Errorf("invalid Linux process group/start time for %d", pid)
	}
	return linuxProcessStat{pid: pid, group: group, state: fields[0][0], startTicks: ticks, threads: threads}, nil
}

const linuxIdentitySettleTimeout = 100 * time.Millisecond
const linuxIdentitySettlePollInterval = time.Millisecond

var errLinuxGroupMembershipChanged = errors.New("Linux process group membership changed during inspection")
var errLinuxLeaderHasLiveThreads = errors.New("terminated Linux leader still has executable threads")
var errLinuxIdentityInspection = errors.New("Linux identity inspection failed")
var errLinuxIdentityMarkersIncomplete = errors.New("live Linux process generation markers are incomplete")

type linuxIdentitySnapshot struct {
	stat     linuxProcessStat
	base     string
	identity string
}

func linuxProcessIdentitySnapshotAt(procRoot string, pid int) (linuxIdentitySnapshot, error) {
	boot, err := os.ReadFile(filepath.Join(procRoot, "sys/kernel/random/boot_id"))
	if err != nil {
		return linuxIdentitySnapshot{}, errors.Join(errLinuxIdentityInspection, fmt.Errorf("read Linux boot identity: %w", err))
	}
	process, err := os.OpenRoot(filepath.Join(procRoot, strconv.Itoa(pid)))
	if err != nil {
		return linuxIdentitySnapshot{}, err
	}
	defer process.Close()
	return linuxProcessIdentitySnapshotFromRoot(strings.TrimSpace(string(boot)), process, pid, procRoot == "/proc" && pid == os.Getpid())
}

// The open proc directory pins one PID object. Relative reads must never use
// the replacement process if this numeric PID is reused during inspection.
func linuxProcessIdentitySnapshotFromRoot(boot string, process *os.Root, pid int, current bool) (linuxIdentitySnapshot, error) {
	readStat := func() (linuxProcessStat, error) {
		raw, err := process.ReadFile("stat")
		if err != nil {
			return linuxProcessStat{}, err
		}
		return parseLinuxProcessStat(raw, pid)
	}
	before, err := readStat()
	if err != nil {
		return linuxIdentitySnapshot{}, err
	}
	environment, environmentErr := process.ReadFile("environ")
	fileToken, fileErr := linuxProcessIdentityFileToken(process, current)
	// Exit can remove environment and descriptors between these reads. Recheck
	// state before interpreting missing markers as a new process generation.
	after, err := readStat()
	if err != nil {
		return linuxIdentitySnapshot{}, err
	}
	if before.startTicks != after.startTicks || before.group != after.group {
		return linuxIdentitySnapshot{}, fmt.Errorf("Linux process generation/group changed during inspection")
	}
	if strings.TrimSpace(boot) == "" || after.startTicks == 0 {
		return linuxIdentitySnapshot{}, fmt.Errorf("invalid Linux boot/start identity")
	}
	base := strings.TrimSpace(boot) + ":" + strconv.FormatUint(after.startTicks, 10)
	snapshot := linuxIdentitySnapshot{stat: after, base: base, identity: base}
	if after.exited() {
		return snapshot, nil
	}
	if after.state == 'Z' || after.state == 'X' || after.state == 'x' {
		return linuxIdentitySnapshot{}, errLinuxLeaderHasLiveThreads
	}
	if err := errors.Join(environmentErr, fileErr); err != nil {
		return linuxIdentitySnapshot{}, errors.Join(errLinuxIdentityInspection, fmt.Errorf("read live Linux process identity markers: %w", err))
	}
	instance, err := linuxProcessInstanceToken(environment)
	if err != nil {
		return linuxIdentitySnapshot{}, err
	}
	if instance != "" {
		snapshot.identity += ":instance:" + instance
	}
	if fileToken != "" {
		snapshot.identity += ":fd:" + fileToken
	}
	return snapshot, nil
}

func platformProcessGroupQuiescent(pgid int) (bool, error) {
	deadline := time.Now().Add(linuxIdentitySettleTimeout)
	for {
		absent, err := linuxProcessGroupQuiescentOnce(pgid)
		if !errors.Is(err, errLinuxGroupMembershipChanged) || !time.Now().Before(deadline) {
			return absent, err
		}
		time.Sleep(linuxIdentitySettlePollInterval)
	}
}

func linuxProcessGroupQuiescentOnce(pgid int) (bool, error) {
	if pgid <= 0 {
		return false, fmt.Errorf("Linux process group must be positive")
	}
	if err := syscall.Kill(-pgid, 0); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return true, nil
		}
		return false, err
	}
	absent, observed, err := linuxProcessGroupInspectionAt("/proc", pgid)
	if errors.Is(err, errLinuxGroupMembershipChanged) {
		// Reaping can remove the last zombie between scans. Only the kernel
		// absence proof can resolve that changing membership as quiescent.
		if signalErr := syscall.Kill(-pgid, 0); errors.Is(signalErr, syscall.ESRCH) {
			return true, nil
		}
	}
	if err != nil || !absent || observed {
		return absent, err
	}
	// An empty proc view cannot disprove the group that kill(0) observed.
	if err := syscall.Kill(-pgid, 0); errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	return false, fmt.Errorf("Linux process group is present but absent from proc inspection")
}

func linuxProcessGroupQuiescentAt(procRoot string, pgid int) (bool, error) {
	absent, _, err := linuxProcessGroupInspectionAt(procRoot, pgid)
	return absent, err
}

func linuxProcessGroupInspectionAt(procRoot string, pgid int) (bool, bool, error) {
	return linuxStableProcessGroupInspection(func() (bool, []string, error) {
		entries, err := os.ReadDir(procRoot)
		if err != nil {
			return false, nil, err
		}
		return linuxScanProcessGroup(procRoot, pgid, entries)
	})
}

// A member can fork after directory enumeration and exit before its stat read.
// Two quiescent scans must cover the same generation set before claiming absence.
func linuxStableProcessGroupInspection(scan func() (bool, []string, error)) (bool, bool, error) {
	absent, members, err := scan()
	if err != nil || !absent {
		return absent, len(members) != 0, err
	}
	again, next, err := scan()
	if err != nil || !again {
		return again, len(next) != 0, err
	}
	if !slices.Equal(members, next) {
		return false, len(next) != 0, errLinuxGroupMembershipChanged
	}
	return true, len(next) != 0, nil
}

func linuxScanProcessGroup(procRoot string, pgid int, entries []os.DirEntry) (bool, []string, error) {
	var members []string
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		stat, err := readLinuxProcessStat(procRoot, pid)
		if processIdentityNotFound(err) {
			// Its unlisted child can still execute after the listed parent
			// has exited and been reaped. A disappeared entry is unstable.
			return false, members, fmt.Errorf("%w: listed process %d disappeared: %v", errLinuxGroupMembershipChanged, pid, err)
		}
		if err != nil {
			return false, members, fmt.Errorf("inspect Linux process group member %d: %w", pid, err)
		}
		if stat.group == pgid {
			members = append(members, strconv.Itoa(pid)+":"+strconv.FormatUint(stat.startTicks, 10))
			if !stat.exited() {
				return false, members, nil
			}
		}
	}
	return true, members, nil
}

func platformAcknowledgedTreeAbsent(ownership Ownership) (bool, error) {
	return linuxAcknowledgedTreeAbsentAt("/proc", ownership)
}

func linuxAcknowledgedTreeAbsentAt(procRoot string, ownership Ownership) (bool, error) {
	// During exit the kernel clears identity markers before stat reaches Z/X.
	// Retry that transition and remaining exiting threads; elapsed time never
	// proves absence. Inspection failures and persistent live threads remain errors.
	deadline := time.Now().Add(linuxIdentitySettleTimeout)
	for {
		absent, err := linuxAcknowledgedTreeAbsentOnceAt(procRoot, ownership)
		if (!errors.Is(err, errLinuxIdentityMarkersIncomplete) && !errors.Is(err, errLinuxLeaderHasLiveThreads)) || !time.Now().Before(deadline) {
			return absent, err
		}
		time.Sleep(linuxIdentitySettlePollInterval)
	}
}

func linuxAcknowledgedTreeAbsentOnceAt(procRoot string, ownership Ownership) (bool, error) {
	current, err := linuxProcessIdentitySnapshotAt(procRoot, ownership.TreeID)
	if err == nil && !current.stat.exited() {
		return linuxLiveGenerationAbsent(current, ownership.TreeIdentityToken)
	}
	if err != nil && !processIdentityNotFound(err) {
		return false, err
	}
	if err == nil && !linuxIdentityHasBase(ownership.TreeIdentityToken, current.base) {
		return true, nil
	}
	var absent bool
	if procRoot == "/proc" {
		absent, err = platformProcessGroupQuiescent(ownership.TreeID)
	} else {
		absent, err = linuxProcessGroupQuiescentAt(procRoot, ownership.TreeID)
	}
	if err != nil {
		return false, err
	}
	// A PGID remains reserved while its descendants survive. If the old group
	// disappeared during inspection, a new leader can now own that numeric PGID.
	// Recheck its generation before the caller may signal the group.
	after, err := linuxProcessIdentitySnapshotAt(procRoot, ownership.TreeID)
	if processIdentityNotFound(err) {
		if absent {
			return true, nil
		}
		return false, linuxVerifyDescendantGenerationAt(procRoot, ownership)
	}
	if err != nil {
		return false, err
	}
	if !after.stat.exited() {
		return linuxLiveGenerationAbsent(after, ownership.TreeIdentityToken)
	}
	if !linuxIdentityHasBase(ownership.TreeIdentityToken, after.base) {
		return true, nil
	}
	if absent {
		return true, nil
	}
	return false, linuxVerifyDescendantGenerationAt(procRoot, ownership)
}

func linuxIdentityHasBase(identity, base string) bool {
	return identity == base || strings.HasPrefix(identity, base+":")
}

// A terminated leader has lost its random markers. Boot/start ticks alone
// cannot authorize signals: a live member must retain the acknowledged marker.
func linuxVerifyDescendantGenerationAt(procRoot string, ownership Ownership) error {
	parts := strings.Split(ownership.TreeIdentityToken, ":")
	instance := ""
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "instance" {
			instance = parts[i+1]
		}
	}
	if instance == "" {
		return fmt.Errorf("terminated Linux leader lineage is unproven")
	}
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		stat, err := readLinuxProcessStat(procRoot, pid)
		if processIdentityNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		if stat.group != ownership.TreeID || stat.exited() {
			continue
		}
		member, err := linuxProcessIdentitySnapshotAt(procRoot, pid)
		if processIdentityNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		if member.stat.group == ownership.TreeID && !member.stat.exited() && (strings.HasSuffix(member.identity, ":instance:"+instance) || strings.Contains(member.identity, ":instance:"+instance+":fd:")) {
			// The leader may have been replaced while reading the member.
			leader, err := linuxProcessIdentitySnapshotAt(procRoot, ownership.TreeID)
			if processIdentityNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if leader.stat.exited() && linuxIdentityHasBase(ownership.TreeIdentityToken, leader.base) {
				return nil
			}
			if leader.identity == ownership.TreeIdentityToken {
				return nil
			}
			return fmt.Errorf("Linux leader generation changed during descendant inspection")
		}
	}
	return fmt.Errorf("live Linux group lacks its acknowledged generation marker")
}

func linuxLiveGenerationAbsent(current linuxIdentitySnapshot, expected string) (bool, error) {
	if current.identity == expected {
		return false, nil
	}
	if strings.HasPrefix(expected, current.base+":") &&
		((strings.Contains(expected, ":instance:") && !strings.Contains(current.identity, ":instance:")) ||
			(strings.Contains(expected, ":fd:") && !strings.Contains(current.identity, ":fd:"))) {
		return false, errLinuxIdentityMarkersIncomplete
	}
	return true, nil
}
