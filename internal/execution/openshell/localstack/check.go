package localstack

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"github.com/google/uuid"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/securefs"
	"net"
	"net/netip"
	"path/filepath"
	"strings"
)

// CheckTLS validates existing callback trust and the dedicated gateway client
// identity without creating a workspace, account store, or provider process.
func CheckTLS(c Config) error {
	if _, err := c.callbackTLS(); err != nil {
		return err
	}
	ca, err := regularFile(c.Gateway.CAFile, 1<<20)
	if err != nil {
		return ErrConfig
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return ErrConfig
	}
	key, err := privateFile(c.Gateway.KeyFile, 64<<10)
	if err != nil {
		return err
	}
	defer clear(key)
	raw, err := regularFile(c.Gateway.CertificateFile, 1<<20)
	if err != nil {
		return ErrConfig
	}
	pair, err := tls.X509KeyPair(raw, key)
	if err != nil || len(pair.Certificate) == 0 {
		return ErrConfig
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return ErrConfig
	}
	intermediates := x509.NewCertPool()
	for _, der := range pair.Certificate[1:] {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return ErrConfig
		}
		intermediates.AddCert(cert)
	}
	if _, err = leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return ErrConfig
	}
	if host, port, err := net.SplitHostPort(c.Gateway.Address); err != nil || host == "" || port == "" {
		return ErrConfig
	}
	return nil
}

// CheckFiles shares startup's protected-file and model validation. It never
// creates or refreshes credentials. Registration ownership is checked by CLI.
func CheckFiles(c Config) error {
	if Validate(c) != nil {
		return ErrConfig
	}
	for _, raw := range []string{c.OwnerID, c.Model.ConnectionID} {
		id, err := uuid.Parse(raw)
		if err != nil || id == uuid.Nil || id.String() != raw {
			return ErrConfig
		}
	}

	if err := CheckTLS(c); err != nil {
		return err
	}
	raw, err := privateFile(c.MasterKeyFile, 128)
	if err != nil {
		return err
	}
	defer clear(raw)
	key, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil || len(key) != 32 {
		return ErrConfig
	}
	clear(key)
	if c.Model.AuthKind == "api_key" {
		for _, raw := range c.Model.AllowedPrivateAddresses {
			if _, err := netip.ParsePrefix(raw); err != nil {
				return ErrConfig
			}
		}
		if c.Model.CAFile != "" {
			if !filepath.IsAbs(c.Model.CAFile) {
				return ErrConfig
			}
			root, err := securefs.OpenRoot(filepath.Dir(c.Model.CAFile))
			if err != nil {
				return ErrConfig
			}
			raw, err := root.ReadRegular(filepath.Base(c.Model.CAFile), 1<<20)
			root.Close()
			if err != nil {
				return ErrConfig
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(raw) {
				return ErrConfig
			}
		}
		if c.Model.Definition.Validate() != nil {
			return ErrConfig
		}
		secret, err := privateFile(c.Model.APIKeyFile, 16384)
		if err != nil {
			return err
		}
		defer clear(secret)
		if _, err = modelproxy.NewBearerCredential(strings.TrimSpace(string(secret))); err != nil {
			return ErrConfig
		}
	}
	return nil
}

// ReadPrivate is the bounded no-follow operator file reader used by setup too.
func ReadPrivate(path string, limit int64) ([]byte, error) { return privateFile(path, limit) }

func regularFile(path string, limit int64) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, ErrConfig
	}
	root, err := securefs.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, ErrConfig
	}
	defer root.Close()
	raw, err := root.ReadRegular(filepath.Base(path), limit)
	if err != nil {
		return nil, ErrConfig
	}
	return raw, nil
}
