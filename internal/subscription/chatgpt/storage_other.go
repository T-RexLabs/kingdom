//go:build !linux && !darwin

package chatgpt

import "context"

type FileVault struct{ *Vault }

func (f *FileVault) Close() error                                       { return nil }
func OpenFileVault(context.Context, string, []byte) (*FileVault, error) { return nil, ErrUnavailable }
