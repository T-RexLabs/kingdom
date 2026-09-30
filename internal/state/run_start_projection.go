package state

// RunStartedAtSelectExpr projects the first observed provider execution across
// all run attempts, including superseded attempts. A retry cannot reset it.
// alias is a trusted SQL workflow_runs alias, never request input.
func RunStartedAtSelectExpr(alias string) string {
	return `(SELECT MIN(NULLIF(TRIM(started_at), '')) FROM attempt_executions
		WHERE run_id = ` + alias + `.run_id AND execution_mode = 'provider_backed')`
}

// RunStartStateSelectExpr distinguishes an absent observation from work which
// never produced provider execution evidence. Setup/lease/launch timestamps and
// terminal status alone cannot establish that the provider actually executed.
// Canonical settlement evidence can establish execution without fabricating its
// missing start timestamp. Export diagnostics are deliberately excluded.
func RunStartStateSelectExpr(alias string) string {
	return `(SELECT CASE
		WHEN COUNT(NULLIF(TRIM(ae.started_at), '')) > 0 THEN 'observed'
		WHEN COALESCE(MAX(CASE WHEN pcs.settlement_id IS NOT NULL
			AND pe.evidence_kind = 'stream_observation'
			AND (json_extract(pe.payload_json, '$.process_exited') = 1
				OR EXISTS (SELECT 1 FROM provider_process_guards pg
					WHERE pg.provider_invocation_id=pcs.provider_invocation_id
					AND pg.owner_kind='decree_attempt' AND pg.attempt_id=ae.attempt_id))
			THEN 1 ELSE 0 END), 0) > 0 THEN 'unrecorded'
		ELSE 'not_started' END
		FROM attempt_executions ae
		LEFT JOIN provider_completion_settlements pcs ON pcs.settlement_id = ae.canonical_settlement_id
		LEFT JOIN provider_completion_evidence pe ON pe.evidence_id = pcs.primary_evidence_id
		WHERE ae.run_id = ` + alias + `.run_id AND ae.execution_mode = 'provider_backed')`
}
