package capability

import (
	"bytes"
	"context"
	"database/sql"

	"kingdom/internal/execution/access"
	"kingdom/internal/execution/providerrequest"
)

// BindProjectTx pins the configured registry identity in a hosted project. It
// must be called by trusted assembly, never from a browser's project settings.
func (b *Registry) BindProjectTx(ctx context.Context, tx *sql.Tx) error {
	project, err := access.Project(ctx, tx)
	if err != nil {
		return err
	}
	if project == "" {
		return ErrDenied
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.closing {
		return ErrOpen
	}
	var id string
	if err = b.db.QueryRowContext(ctx, `SELECT id FROM worker_capability_registry_identity WHERE singleton=1`).Scan(&id); err != nil {
		return ErrOpen
	}
	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO remote_capability_registry(singleton,registry_id) VALUES(1,?)`, id); err != nil {
		return err
	}
	var saved string
	if err = tx.QueryRowContext(ctx, `SELECT registry_id FROM remote_capability_registry WHERE singleton=1`).Scan(&saved); err != nil {
		return err
	}
	if saved != id {
		return ErrDenied
	}
	return nil
}

// RecordClosureTx accepts only a registry-created proof and revalidates its
// exact historical project identity. It performs no network or live access
// checks, so revoked membership cannot prevent recording cleanup evidence.
func RecordClosureTx(ctx context.Context, tx *sql.Tx, proof ClosureProof) error {
	if len(proof.certificate) == 0 {
		return ErrDenied
	}
	cert, digest, err := providerrequest.ValidateRemoteCapabilityClosure(ctx, tx, proof.certificate)
	if err != nil {
		return err
	}
	if digest != proof.digest {
		return ErrDenied
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO remote_capability_closures(allocation,purpose,registry_id,certificate_id,certificate,certificate_sha256)VALUES(?,?,?,?,?,?)`, cert.Binding.Runtime.Allocation, cert.Purpose, cert.RegistryID, cert.CertificateID, proof.certificate, digest)
	if err != nil {
		return err
	}
	var raw []byte
	var saved string
	if err = tx.QueryRowContext(ctx, `SELECT certificate,certificate_sha256 FROM remote_capability_closures WHERE allocation=? AND purpose=?`, cert.Binding.Runtime.Allocation, cert.Purpose).Scan(&raw, &saved); err != nil {
		return err
	}
	if !bytes.Equal(raw, proof.certificate) || saved != digest {
		return ErrDenied
	}
	return nil
}
