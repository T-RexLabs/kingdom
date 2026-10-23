package orchestration

import "testing"

func TestFreshRuntimePublishesBundledWorkflowBeforeStart(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	rt, err := NewRuntime(Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), Provider: memoryProvider(db)})
	if err != nil {
		t.Fatal(err)
	}
	_ = rt
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM workflow_definitions d JOIN workflow_nodes n ON n.workflow_definition_id=d.workflow_definition_id WHERE d.workflow_name='work-order-v1' AND d.workflow_version='seed-v1'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("fresh runtime has no published bundled workflow graph")
	}
}

func TestBundledWorkflowBootstrapIsExactAndRejectsChangedGraph(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	cfg := Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), Provider: memoryProvider(db)}
	if _, err := NewRuntime(cfg); err != nil {
		t.Fatal(err)
	}
	var first int
	if err := db.QueryRow(`SELECT count(*) FROM workflow_definitions`).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRuntime(cfg); err != nil {
		t.Fatal("exact bootstrap failed", err)
	}
	var after int
	if err := db.QueryRow(`SELECT count(*) FROM workflow_definitions`).Scan(&after); err != nil || after != first {
		t.Fatal("bootstrap duplicated definitions", err)
	}
	if _, err := db.Exec(`UPDATE workflow_nodes SET node_class='execution_kickoff' WHERE workflow_definition_id IN(SELECT workflow_definition_id FROM workflow_definitions WHERE workflow_version='seed-v1') AND node_class<>'execution_kickoff'`); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRuntime(cfg); err == nil {
		t.Fatal("changed retained graph accepted")
	}
}
