//go:build darwin || linux

package securefs

import "golang.org/x/sys/unix"

func fileIdentity(stat unix.Stat_t) FileIdentity {
	return FileIdentity{
		Device:                uint64(stat.Dev),
		Inode:                 uint64(stat.Ino),
		SizeBytes:             stat.Size,
		ModTimeSeconds:        stat.Mtim.Sec,
		ModTimeNanoseconds:    stat.Mtim.Nsec,
		ChangeTimeSeconds:     stat.Ctim.Sec,
		ChangeTimeNanoseconds: stat.Ctim.Nsec,
	}
}
