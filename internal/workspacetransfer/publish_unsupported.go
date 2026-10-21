//go:build !linux && !darwin

package workspacetransfer

import "errors"

func publishDirectory(string, string) error { return errors.New("workspace helper requires Linux") }
