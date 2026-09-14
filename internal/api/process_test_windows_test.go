//go:build windows

package api

import "golang.org/x/sys/windows"

func providerTestProcessAbsent(pid int) bool {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return true
	}
	windows.CloseHandle(handle)
	return false
}
