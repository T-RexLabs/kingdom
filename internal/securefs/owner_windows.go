//go:build windows

package securefs

import "io/fs"

// Windows ownership validation is performed by the ACL-protected runtime root;
// POSIX uid metadata is unavailable through FileInfo on this platform.
func CurrentUserOwns(fs.FileInfo) error { return nil }
