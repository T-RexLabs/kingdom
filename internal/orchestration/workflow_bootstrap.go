package orchestration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"kingdom/internal/bootstrapassets"
	"kingdom/internal/core/schemas"
	"kingdom/internal/core/workflow"
	"kingdom/internal/state"
	"time"
)

func initializeBundledWorkflows(ctx context.Context, db *sql.DB, schemaSet *schemas.Set, now time.Time) error {
	seeds, err := bootstrapassets.ListWorkflowSeeds()
	if err != nil {
		return err
	}
	compiler := workflow.NewCompiler(schemaSet, workflow.NewParser(state.CanonicalRoleIDs()))
	compiled := make([]*workflow.CompileResult, 0, len(seeds))
	for _, seed := range seeds {
		raw, err := json.Marshal(seed.Definition)
		if err != nil {
			return err
		}
		result, err := compiler.CompileJSON(seed.WorkflowID, raw)
		if err != nil {
			return err
		}
		compiled = append(compiled, result)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, result := range compiled {
		identity := sha256.Sum256([]byte("seed-v1\x00" + result.SHA256))
		if _, err = state.PublishCompiledWorkflowTx(ctx, tx, "workflow_seed_"+hex.EncodeToString(identity[:]), result.Definition.WorkflowID, "seed-v1", result, now.UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	return tx.Commit()
}
