//go:build !linux && !darwin

package providerlauncher

import "errors"

func checkProviderTrust() error {
	return errors.New("provider trust validation requires a supported platform")
}
