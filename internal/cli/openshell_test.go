package cli

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"github.com/google/uuid"
	"io"
	"kingdom/internal/execution/openshell/deployment"
	"kingdom/internal/execution/openshell/localstack"
	"kingdom/internal/subscription/chatgpt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func openShellFixture(t *testing.T) (openShellOptions, deployment.Bundle) {
	t.Helper()
	root, rootErr := filepath.EvalSymlinks(t.TempDir())
	if rootErr != nil {
		t.Fatal(rootErr)
	}
	home := filepath.Join(root, ".kingdom")
	if e := os.Mkdir(home, 0755); e != nil {
		t.Fatal(e)
	}
	cfg := defaultInstallConfig(home)
	if e := writeInstallConfig(filepath.Join(home, "config.yaml"), cfg, false); e != nil {
		t.Fatal(e)
	}
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	der, e := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	certPath := filepath.Join(root, "cert.pem")
	keyPath := filepath.Join(root, "key.pem")
	_ = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600)
	_ = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600)
	h := strings.Repeat("a", 64)
	b := deployment.Bundle{Kind: deployment.Kind, Version: 1, Gateway: deployment.Gateway{ID: "gateway", Address: "localhost:59563", Image: "fixture/gateway@sha256:" + h, CAFile: certPath, CertificateFile: certPath, KeyFile: keyPath}, Profile: deployment.Profile{SchemaVersion: 1, GatewayBinarySHA256: h, SupervisorImage: "fixture/supervisor@sha256:" + h, SandboxRuntimeImage: "fixture/runtime@sha256:" + h, SandboxRuntimeBinarySHA256: h}, Worker: deployment.Worker{Image: "fixture/worker@sha256:" + h, LauncherSHA256: h, OpenCodeSHA256: h, WorkspaceHelperSHA256: h, FileToolSHA256: h}, WorkerID: "worker", Callback: deployment.Callback{Listen: "0.0.0.0:9443", Origin: "https://localhost:9443", CertificateFile: certPath, KeyFile: keyPath, CAFile: certPath, AllowedIPs: []string{"127.0.0.1"}}, Limits: deployment.Limits{Context: 128000, Output: 16000, Lifetime: 3600}}
	b.ProfileSHA256 = b.Profile.Digest()
	bundlePath := filepath.Join(root, "bundle.json")
	raw, _ := json.Marshal(b)
	_ = os.WriteFile(bundlePath, raw, 0600)
	apiKey := filepath.Join(root, "model.key")
	_ = os.WriteFile(apiKey, []byte("synthetic-test-key"), 0600)
	return openShellOptions{Config: filepath.Join(home, "config.yaml"), Bundle: bundlePath, APIKeyFile: apiKey, Model: "gpt-6-luna", Thinking: "medium"}, b
}
func passingDeploymentCheck(context.Context, deployment.Bundle) (deployment.CheckResult, error) {
	return deployment.CheckResult{BundleVerified: true, GatewayAuthenticated: true, DockerDriverAvailable: true}, nil
}
func TestOpenShellConfigurePreservesExactReplayAndRejectsConflicts(t *testing.T) {
	o, _ := openShellFixture(t)
	if e := configureOpenShell(context.Background(), io.Discard, o, passingDeploymentCheck); e != nil {
		t.Fatal(e)
	}
	cfg, e := readInstallConfig(o.Config)
	if e != nil {
		t.Fatal(e)
	}
	operator, e := localstack.Load(cfg.Execution.ManagedConfig)
	if e != nil {
		t.Fatal(e)
	}
	paths := []string{o.Config, cfg.Execution.ManagedConfig, filepath.Join(operator.StateRoot, "setup.json"), operator.MasterKeyFile}
	before := map[string][]byte{}
	for _, p := range paths {
		before[p], e = os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
	}
	if e = configureOpenShell(context.Background(), io.Discard, o, passingDeploymentCheck); e != nil {
		t.Fatal(e)
	}
	for _, p := range paths {
		after, _ := os.ReadFile(p)
		if !bytes.Equal(after, before[p]) {
			t.Fatal("replay changed retained state", p)
		}
	}
	changed := o
	changed.Model = "other"
	if configureOpenShell(context.Background(), io.Discard, changed, passingDeploymentCheck) == nil {
		t.Fatal("conflicting configuration accepted")
	}
	for _, p := range paths {
		after, _ := os.ReadFile(p)
		if !bytes.Equal(after, before[p]) {
			t.Fatal("conflict changed state", p)
		}
	}
	if operator.Model.Definition.Endpoint != "https://api.openai.com/v1" || operator.Model.Definition.Provider != "openai" || operator.OwnerID == "" || operator.Model.ConnectionID == "" {
		t.Fatal("incorrect local authority")
	}
}
func TestOpenShellInterruptedProjectPublicationKeepsOneUseIdentities(t *testing.T) {
	o, _ := openShellFixture(t)
	check := func(ctx context.Context, b deployment.Bundle) (deployment.CheckResult, error) {
		raw, _ := os.ReadFile(o.Config)
		_ = os.WriteFile(o.Config, append(raw, []byte("\n# concurrent edit\n")...), 0644)
		return passingDeploymentCheck(ctx, b)
	}
	if configureOpenShell(context.Background(), io.Discard, o, check) == nil {
		t.Fatal("changed project configuration was overwritten")
	}
	root := filepath.Join(filepath.Dir(o.Config), "managed")
	before, e := readSetupRecord(root)
	if e != nil {
		t.Fatal(e)
	}
	keyBefore, _ := os.ReadFile(before.Operator.MasterKeyFile)
	if e = configureOpenShell(context.Background(), io.Discard, o, passingDeploymentCheck); e != nil {
		t.Fatal(e)
	}
	after, e := readSetupRecord(root)
	if e != nil {
		t.Fatal(e)
	}
	keyAfter, _ := os.ReadFile(after.Operator.MasterKeyFile)
	if !reflect.DeepEqual(before, after) || !bytes.Equal(keyBefore, keyAfter) {
		t.Fatal("interrupted setup regenerated authority")
	}
}
func TestOpenShellUnsafeSecretAndConfigurationLockFailBeforePublication(t *testing.T) {
	o, _ := openShellFixture(t)
	original, _ := os.ReadFile(o.Config)
	_ = os.Chmod(o.APIKeyFile, 0644)
	if configureOpenShell(context.Background(), io.Discard, o, passingDeploymentCheck) == nil {
		t.Fatal("public secret accepted")
	}
	if _, e := os.Stat(filepath.Join(filepath.Dir(o.Config), "managed")); !os.IsNotExist(e) {
		t.Fatal("invalid setup published state")
	}
	after, _ := os.ReadFile(o.Config)
	if !bytes.Equal(original, after) {
		t.Fatal("invalid setup changed project")
	}
	release, e := lockProjectConfiguration(o.Config)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	called := false
	check := func(context.Context, deployment.Bundle) (deployment.CheckResult, error) {
		called = true
		return deployment.CheckResult{}, nil
	}
	if configureOpenShell(context.Background(), io.Discard, o, check) == nil || called {
		t.Fatal("configuration ignored runtime lock")
	}
	// The runtime must take the same lock before parsing the project config.
	_ = os.WriteFile(o.Config, []byte("invalid: ["), 0644)
	e = runSummon(context.Background(), io.Discard, io.Discard, []string{"--config", o.Config})
	if e == nil || !strings.Contains(e.Error(), "stop the project runtime") {
		t.Fatalf("foreground startup bypassed configuration lock: %v", e)
	}
}

func TestOpenShellHelpAndCommonModelContract(t *testing.T) {
	for _, topic := range [][]string{{"openshell"}, {"openshell", "export"}, {"openshell", "configure"}, {"openshell", "check"}} {
		var out bytes.Buffer
		writeCommandHelp(&out, topic)
		if !strings.Contains(out.String(), "kd openshell") {
			t.Fatal("missing setup help", out.String())
		}
	}
	m, e := setupModel(openShellOptions{APIKeyFile: "/private/key", Model: "gpt-6-luna", Thinking: "medium", ModelCA: "/private/ca", ModelPrivate: []string{"127.0.0.1/32"}})
	if e != nil || m.CAFile != "/private/ca" || len(m.AllowedPrivateAddresses) != 1 {
		t.Fatal("explicit trust lost", e)
	}
	if _, e = setupModel(openShellOptions{APIKeyFile: "/private/key", Model: "model", ModelProfile: "/profile"}); e == nil {
		t.Fatal("ambiguous model inputs admitted")
	}
	if _, e = setupModel(openShellOptions{APIKeyFile: "/private/key", Model: "model", ModelPrivate: []string{"localhost"}}); e == nil {
		t.Fatal("implicit private address accepted")
	}
}

func TestOpenShellChatGPTSetupResumesIdentityJournalBeforeOperatorFile(t *testing.T) {
	o, _ := openShellFixture(t)
	original, _ := os.ReadFile(o.Config)
	stateRoot, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	_ = os.Chmod(stateRoot, 0700)
	master := bytes.Repeat([]byte{9}, 32)
	masterPath := filepath.Join(stateRoot, "master.key")
	_ = os.WriteFile(masterPath, []byte(hex.EncodeToString(master)+"\n"), 0600)
	owner := uuid.NewString()
	key := localstack.OwnerCredentialKey(master, owner)
	path := filepath.Join(stateRoot, "chatgpt.db")
	db, e := sql.Open("sqlite", path)
	if e != nil {
		t.Fatal(e)
	}
	_, e = chatgpt.NewVault(context.Background(), db, key)
	clear(key)
	if e != nil {
		t.Fatal(e)
	}
	_, e = db.Exec(`INSERT INTO chatgpt_registrations(id,owner_id,client_id,subject,revision,token_version,state) VALUES('registration',?,'oaiapp_fixture','subject',1,1,'ready')`, owner)
	if e != nil {
		t.Fatal(e)
	}
	db.Close()
	_ = os.Chmod(path, 0600)
	o.APIKeyFile = ""
	o.Model = ""
	o.Thinking = ""
	o.StateRoot = stateRoot
	o.Owner = owner
	o.MasterKey = masterPath
	o.Registration = "registration"
	if e = configureOpenShell(context.Background(), io.Discard, o, passingDeploymentCheck); e != nil {
		t.Fatal(e)
	}
	before, e := readSetupRecord(stateRoot)
	if e != nil {
		t.Fatal(e)
	}
	_ = os.Remove(filepath.Join(stateRoot, "operator.json"))
	_ = os.WriteFile(o.Config, original, 0644)
	if e = configureOpenShell(context.Background(), io.Discard, o, passingDeploymentCheck); e != nil {
		t.Fatal(e)
	}
	after, e := readSetupRecord(stateRoot)
	if e != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("interrupted account setup changed identity", e)
	}
}

func TestOpenShellConfigureUsesCanonicalBackendDebtGuard(t *testing.T) {
	o, _ := openShellFixture(t)
	cfg, e := readInstallConfig(o.Config)
	if e != nil {
		t.Fatal(e)
	}
	if e = ensureDatabaseSchemaWithContext(context.Background(), cfg.DBPath); e != nil {
		t.Fatal(e)
	}
	db, e := sql.Open("sqlite", cfg.DBPath)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	_, e = db.Exec(`INSERT INTO work_orders(work_order_id,work_order_title,work_order_text,metadata_json,created_at) VALUES('order','Task','Task','{}','now');INSERT INTO workflow_definitions(workflow_definition_id,workflow_name,workflow_version,definition_json,created_at) VALUES('workflow','Test','1','{}','now');INSERT INTO workflow_runs(run_id,work_order_id,workflow_definition_id,run_status,plan_state,row_version,created_at,updated_at) VALUES('run','order','workflow','running','approved',1,'now','now')`)
	if e != nil {
		t.Fatal(e)
	}
	original, _ := os.ReadFile(o.Config)
	if configureOpenShell(context.Background(), io.Discard, o, passingDeploymentCheck) == nil {
		t.Fatal("active local work was switched")
	}
	after, _ := os.ReadFile(o.Config)
	if !bytes.Equal(original, after) {
		t.Fatal("active project configuration changed")
	}
	_, e = db.Exec(`UPDATE workflow_runs SET run_status='succeeded'`)
	if e != nil {
		t.Fatal(e)
	}
	if e = configureOpenShell(context.Background(), io.Discard, o, passingDeploymentCheck); e != nil {
		t.Fatal(e)
	}
	var bindings int
	_ = db.QueryRow(`SELECT count(*) FROM execution_access_local_owner`).Scan(&bindings)
	if bindings != 0 {
		t.Fatal("configuration preflight committed its provisional owner binding")
	}
}

func TestOpenShellInvalidTLSFailsBeforeGatewayOrStatePublication(t *testing.T) {
	for _, scenario := range []string{"expired", "hostname", "key-mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			o, b := openShellFixture(t)
			original, _ := os.ReadFile(o.Config)
			raw, _ := os.ReadFile(b.Callback.CertificateFile)
			block, _ := pem.Decode(raw)
			cert, e := x509.ParseCertificate(block.Bytes)
			if e != nil {
				t.Fatal(e)
			}
			raw, _ = os.ReadFile(b.Callback.KeyFile)
			block, _ = pem.Decode(raw)
			key, e := x509.ParseECPrivateKey(block.Bytes)
			if e != nil {
				t.Fatal(e)
			}
			switch scenario {
			case "expired":
				cert.NotBefore = time.Now().Add(-2 * time.Hour)
				cert.NotAfter = time.Now().Add(-time.Hour)
			case "hostname":
				cert.DNSNames = []string{"other.example"}
			case "key-mismatch":
				key, _ = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			}
			if scenario == "key-mismatch" {
				raw, _ = x509.MarshalECPrivateKey(key)
				_ = os.WriteFile(b.Callback.KeyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: raw}), 0600)
			} else {
				der, e := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
				if e != nil {
					t.Fatal(e)
				}
				_ = os.WriteFile(b.Callback.CertificateFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600)
			}
			called := false
			check := func(context.Context, deployment.Bundle) (deployment.CheckResult, error) {
				called = true
				return deployment.CheckResult{}, nil
			}
			if configureOpenShell(context.Background(), io.Discard, o, check) == nil || called {
				t.Fatal("invalid TLS reached the gateway")
			}
			after, _ := os.ReadFile(o.Config)
			if !bytes.Equal(original, after) {
				t.Fatal("invalid TLS changed project configuration")
			}
			if _, e = os.Stat(filepath.Join(filepath.Dir(o.Config), "managed")); !os.IsNotExist(e) {
				t.Fatal("invalid TLS published managed state")
			}
		})
	}
}
