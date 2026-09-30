package state

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func TestArtifactModeContractAndPersistence(t *testing.T) {
	for _, tc := range []struct {
		name, action string
		mode         *int64
		valid        bool
	}{
		{"write-required", "write", nil, false},
		{"write-zero", "write", artifactTestMode(0), true},
		{"write-private-executable", "write", artifactTestMode(0700), true},
		{"write-special-bits", "write", artifactTestMode(04755), false},
		{"write-negative", "write", artifactTestMode(-1), false},
		{"delete-absent", "delete", nil, true},
		{"delete-present", "delete", artifactTestMode(0644), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			artifact := WorkerArtifact{Action: tc.action, Name: "file", MIME: "text/plain", Path: "file", ModeBits: tc.mode}
			raw, err := json.Marshal(artifact)
			if err != nil {
				t.Fatal(err)
			}
			req := WorkerOutputValidationRequest{RunID: "run_1", NodeRunID: "nr_1", AttemptID: "mode-attempt", ProducerRole: "governance", CreatedAt: "2026-03-01T12:00:00Z", ArtifactContentByPath: map[string][]byte{"file": []byte("content")}, OutputJSON: []byte(fmt.Sprintf(`{"schema_version":"v1","status":"succeeded","summary":"done","payload_json":{},"artifacts":[%s],"error_reason":null}`, raw))}
			_, err = NewWorkerOutputValidator(db, loadSchemaSet(t)).ValidateAndPersist(context.Background(), req)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
			if !tc.valid {
				if got := countArtifactsForAttempt(t, db, req.AttemptID); got != 0 {
					t.Fatalf("invalid mode persisted %d rows", got)
				}
				return
			}
			record, err := NewArtifactService(db).GetArtifact(context.Background(), "mode-attempt_artifact_1")
			if err != nil {
				t.Fatal(err)
			}
			if tc.mode == nil {
				if record.ModeBits != nil {
					t.Fatal("deletion retained mode")
				}
			} else if record.ModeBits == nil || *record.ModeBits != *tc.mode {
				t.Fatalf("mode=%v want=%o", record.ModeBits, *tc.mode)
			}
		})
	}
}

func TestArtifactModeDatabaseConstraints(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	for _, tc := range []struct {
		action string
		mode   any
		valid  bool
	}{{"write", nil, false}, {"write", 0, true}, {"write", 0700, true}, {"write", 01000, false}, {"write", -1, false}, {"delete", nil, true}, {"delete", 0644, false}} {
		_, err := db.Exec(`INSERT INTO artifacts(artifact_id,run_id,node_run_id,attempt_id,artifact_action,name,mime,path,sha256,size_bytes,mode_bits,producer_role,created_at) VALUES ('mode-row','run_1','nr_1','mode-attempt',?,'file','text/plain','file','',0,?,'governance','2026-03-01T12:00:00Z')`, tc.action, tc.mode)
		if (err == nil) != tc.valid {
			t.Fatalf("action=%s mode=%v valid=%v err=%v", tc.action, tc.mode, tc.valid, err)
		}
		if _, err := db.Exec(`DELETE FROM artifacts WHERE artifact_id='mode-row'`); err != nil {
			t.Fatal(err)
		}
	}
}
