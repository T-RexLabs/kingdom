package chatgpt

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"database/sql"
	"strings"
)

func validVaultProof(aead cipher.AEAD, version int, host string, proof []byte) bool {
	if version != 1 || !strings.HasPrefix(host, "urn:uuid:") || len(proof) < aead.NonceSize() || len(proof) > 128<<10 {
		return false
	}
	plain, err := aead.Open(nil, proof[:aead.NonceSize()], proof[aead.NonceSize():], []byte(host))
	defer clear(plain)
	return err == nil && string(plain) == "kingdom-chatgpt-key/v1"
}

// CheckRegistration verifies an existing local authorization using SELECT only.
// It neither opens/migrates storage nor reads tokens, refreshes, or recovers it.
func CheckRegistration(ctx context.Context, db *sql.DB, key []byte, owner, id string) error {
	if db == nil || len(key) != 32 || !textID(owner) || !textID(id) {
		return ErrInvalid
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return ErrInvalid
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return ErrInvalid
	}
	var version int
	var host string
	var proof []byte
	if db.QueryRowContext(ctx, `SELECT version,host_id,key_proof FROM chatgpt_vault WHERE slot=1`).Scan(&version, &host, &proof) != nil || !validVaultProof(aead, version, host, proof) {
		return ErrDenied
	}
	var revision int64
	var state, client, subject string
	if db.QueryRowContext(ctx, `SELECT revision,state,client_id,subject FROM chatgpt_registrations WHERE id=? AND owner_id=?`, id, owner).Scan(&revision, &state, &client, &subject) != nil || revision < 1 || state != "ready" || !clientID(client) || !textID(subject) {
		return ErrDenied
	}
	var fenced int
	if db.QueryRowContext(ctx, `SELECT count(*) FROM chatgpt_exports WHERE registration_id=? AND revision=?`, id, revision).Scan(&fenced) != nil || fenced != 0 {
		return ErrDenied
	}
	return nil
}
