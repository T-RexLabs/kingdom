package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
	"io"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/openshell/deployment"
	"kingdom/internal/execution/openshell/localstack"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/securefs"
	"kingdom/internal/subscription/chatgpt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"time"
)

type setupRecord struct {
	Version         int               `json:"version"`
	Fingerprint     string            `json:"fingerprint"`
	Bundle          deployment.Bundle `json:"bundle"`
	Operator        localstack.Config `json:"operator"`
	MasterKeySHA256 string            `json:"master_key_sha256"`
	ProjectConfig   string            `json:"project_config"`
}

func sumBytes(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
func readSetupRecord(root string) (setupRecord, error) {
	var r setupRecord
	if e := setupDir(root); e != nil {
		return r, e
	}
	raw, e := localstack.ReadPrivate(filepath.Join(root, "setup.json"), 256<<10)
	if e != nil {
		return r, e
	}
	if e = deployment.StrictJSON(raw, &r); e != nil {
		return r, e
	}
	if r.Version != 1 || r.Bundle.Validate() != nil {
		return r, deployment.ErrInvalid
	}
	return r, nil
}
func verifySetupRecord(r setupRecord, c localstack.Config) error {
	if r.Version != 1 || !reflect.DeepEqual(r.Operator, c) || r.Bundle.Validate() != nil {
		return errors.New("managed setup does not match its retained operator configuration")
	}
	expected := r.Bundle.LocalConfig()
	expected.OwnerID = c.OwnerID
	expected.StateRoot = c.StateRoot
	expected.MasterKeyFile = c.MasterKeyFile
	expected.Model = c.Model
	if !reflect.DeepEqual(expected, c) {
		return errors.New("operator deployment differs from its retained bundle")
	}
	raw, e := localstack.ReadPrivate(c.MasterKeyFile, 128)
	if e != nil {
		return e
	}
	defer clear(raw)
	if sumBytes(raw) != r.MasterKeySHA256 {
		return errors.New("managed master key differs from the retained setup")
	}
	return nil
}
func configureOpenShell(ctx context.Context, out io.Writer, o openShellOptions, check func(context.Context, deployment.Bundle) (deployment.CheckResult, error)) error {
	configPath := absoluteOption(o.Config)
	release, e := lockProjectConfiguration(configPath)
	if e != nil {
		return e
	}
	defer release()
	configRoot, e := securefs.OpenRoot(filepath.Dir(configPath))
	if e != nil {
		return e
	}
	defer configRoot.Close()
	original, e := configRoot.ReadRegular(filepath.Base(configPath), 256<<10)
	if e != nil {
		return e
	}
	cfg, e := readInstallConfig(configPath)
	if e != nil {
		return e
	}
	if _, active, e := findSummonInstanceForConfig(configPath, cfg.KingdomHome); e != nil || active {
		return errors.New("stop the project runtime before configuring OpenShell")
	}
	raw, e := localstack.ReadPrivate(absoluteOption(o.Bundle), 256<<10)
	if e != nil {
		return e
	}
	bundle, e := deployment.Decode(raw)
	if e != nil {
		return e
	}
	model, e := setupModel(o)
	if e != nil {
		return e
	}
	stateRoot := absoluteOption(o.StateRoot)
	if stateRoot == "" {
		stateRoot = filepath.Join(cfg.KingdomHome, "managed")
	}
	o.MasterKey = absoluteOption(o.MasterKey)
	requestRaw, _ := json.Marshal(struct {
		Bundle                      deployment.Bundle
		Model                       localstack.ModelConfig
		StateRoot, Owner, MasterKey string
	}{bundle, model, stateRoot, o.Owner, o.MasterKey})
	fingerprint := sumBytes(requestRaw)
	var record setupRecord
	var existing, missingOperator bool
	if _, statErr := os.Lstat(filepath.Join(stateRoot, "setup.json")); statErr == nil {
		record, e = readSetupRecord(stateRoot)
		if e != nil {
			return e
		}
		existing = true
		if record.Fingerprint != fingerprint || record.ProjectConfig != configPath {
			return errors.New("managed setup already exists with different inputs; existing identities were preserved")
		}
	} else if !os.IsNotExist(statErr) {
		return statErr
	}
	if existing {
		if e = verifySetupRecord(record, record.Operator); e != nil {
			return e
		}
		operatorPath := filepath.Join(stateRoot, "operator.json")
		if _, statErr := os.Lstat(operatorPath); os.IsNotExist(statErr) && record.Operator.Model.AuthKind == "chatgpt" && cfg.Execution.Backend == "local" {
			missingOperator = true
		} else {
			saved, loadErr := localstack.Load(operatorPath)
			if loadErr != nil {
				return loadErr
			}
			if !reflect.DeepEqual(saved, record.Operator) {
				return errors.New("retained operator configuration changed")
			}
		}
	}
	operator := bundle.LocalConfig()
	if existing {
		operator = record.Operator
	} else {
		operator.StateRoot = stateRoot
		operator.OwnerID = uuid.NewString()
		operator.MasterKeyFile = filepath.Join(stateRoot, "master.key")
		operator.Model = model
		operator.Model.ConnectionID = uuid.NewString()
		if model.AuthKind == "chatgpt" {
			if id, e := uuid.Parse(o.Owner); e != nil || id.String() != o.Owner || id == uuid.Nil {
				return errors.New("invalid existing ChatGPT owner")
			}
			operator.OwnerID = o.Owner
			operator.MasterKeyFile = o.MasterKey
		}
	}
	if cfg.Execution.Backend == "openshell" && cfg.Execution.ManagedConfig != filepath.Join(stateRoot, "operator.json") {
		return errors.New("project already uses a different managed operator; keep it for canonical cleanup")
	}
	if e = checkBackendTransition(ctx, cfg, operator.OwnerID); e != nil {
		return e
	}
	if e = localstack.Validate(operator); e != nil {
		return e
	}
	if e = localstack.CheckTLS(operator); e != nil {
		return e
	}
	probe, cancel := context.WithTimeout(ctx, 15*time.Second)
	_, e = check(probe, bundle)
	cancel()
	if e != nil {
		return fmt.Errorf("OpenShell deployment check failed: %w", e)
	}
	if !existing {
		record = setupRecord{Version: 1, Fingerprint: fingerprint, Bundle: bundle, Operator: operator, ProjectConfig: configPath}
		if model.AuthKind == "chatgpt" {
			if e = setupDir(stateRoot); e != nil {
				return e
			}
			if e = localstack.CheckFiles(operator); e != nil {
				return e
			}
			if e = checkSetupChatGPT(ctx, operator); e != nil {
				return e
			}
			key, e := localstack.ReadPrivate(operator.MasterKeyFile, 128)
			if e != nil {
				return e
			}
			record.MasterKeySHA256 = sumBytes(key)
			clear(key)
			if e = writeSetupRecord(stateRoot, record, nil); e != nil {
				return e
			}
		} else {
			if _, e = os.Lstat(stateRoot); !os.IsNotExist(e) {
				return errors.New("managed state destination already exists without a matching setup record")
			}
			parent, e := securefs.OpenRoot(filepath.Dir(stateRoot))
			if e != nil {
				return e
			}
			parent.Close()
			stage, e := os.MkdirTemp(filepath.Dir(stateRoot), ".kingdom-managed-stage-")
			if e != nil {
				return e
			}
			defer os.RemoveAll(stage)
			master := make([]byte, 32)
			if _, e = rand.Read(master); e != nil {
				return e
			}
			key := []byte(hex.EncodeToString(master) + "\n")
			clear(master)
			defer clear(key)
			record.MasterKeySHA256 = sumBytes(key)
			if e = writeSetupRecord(stage, record, key); e != nil {
				return e
			}
			validation := operator
			validation.MasterKeyFile = filepath.Join(stage, "master.key")
			if e = localstack.CheckFiles(validation); e != nil {
				return e
			}
			if e = publishSetupDirectory(stage, stateRoot); e != nil {
				return e
			}
			if e = syncSetupParent(stateRoot); e != nil {
				return e
			}
		}
	}
	if e = localstack.CheckFiles(operator); e != nil {
		return e
	}
	if e = checkSetupChatGPT(ctx, operator); e != nil {
		return e
	}
	if missingOperator {
		if e = writeSetupRecord(stateRoot, record, nil); e != nil {
			return e
		}
	}
	if cfg.Execution.Backend == "openshell" && cfg.Execution.ManagedConfig == filepath.Join(stateRoot, "operator.json") {
		_, e = fmt.Fprintln(out, "OpenShell is already configured. Existing owner, connection, and key were preserved.")
		return e
	}
	if e = publishOpenShellConfiguration(configRoot, configPath, original, filepath.Join(stateRoot, "operator.json")); e != nil {
		return fmt.Errorf("private setup retained; rerun the same command to finish project configuration: %w", e)
	}
	_, e = fmt.Fprintln(out, "OpenShell configured for this project. Start it with kd summon. Gateway and worker execution pins are enforced on the first managed allocation.")
	return e
}
func writeSetupRecord(root string, r setupRecord, key []byte) error {
	dir, e := securefs.OpenRoot(root)
	if e != nil {
		return e
	}
	defer dir.Close()
	// The durable identity record comes first when attaching an existing account
	// vault. Replays must reuse it before filling an interrupted operator file.
	raw, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		return e
	}
	if _, e = dir.CreateRegularExact("setup.json", append(raw, '\n'), 0600, 0700); e != nil {
		return e
	}
	if key != nil {
		if _, e = dir.CreateRegularExact("master.key", key, 0600, 0700); e != nil {
			return e
		}
	}
	raw, e = json.MarshalIndent(r.Operator, "", "  ")
	if e != nil {
		return e
	}
	_, e = dir.CreateRegularExact("operator.json", append(raw, '\n'), 0600, 0700)
	return e
}
func publishOpenShellConfiguration(root *securefs.Root, path string, original []byte, operatorPath string) error {
	current, e := root.ReadRegular(filepath.Base(path), 256<<10)
	if e != nil {
		return e
	}
	if !bytes.Equal(current, original) {
		return errors.New("project configuration changed during setup")
	}
	var doc yaml.Node
	if e = yaml.Unmarshal(original, &doc); e != nil {
		return e
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return errors.New("project configuration must be a YAML mapping")
	}
	mapping := doc.Content[0]
	var execution *yaml.Node
	for i := 0; i < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value == "execution" {
			execution = mapping.Content[i+1]
		}
	}
	if execution == nil {
		execution = &yaml.Node{Kind: yaml.MappingNode}
		mapping.Content = append(mapping.Content, &yaml.Node{Kind: yaml.ScalarNode, Value: "execution"}, execution)
	}
	if execution.Kind != yaml.MappingNode {
		return errors.New("execution must be a YAML mapping")
	}
	set := func(key, value string) {
		for i := 0; i < len(execution.Content); i += 2 {
			if execution.Content[i].Value == key {
				execution.Content[i+1] = &yaml.Node{Kind: yaml.ScalarNode, Value: value}
				return
			}
		}
		execution.Content = append(execution.Content, &yaml.Node{Kind: yaml.ScalarNode, Value: key}, &yaml.Node{Kind: yaml.ScalarNode, Value: value})
	}
	set("backend", "openshell")
	set("managed_config", operatorPath)
	raw, e := yaml.Marshal(&doc)
	if e != nil {
		return e
	}
	var candidate installConfig
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if decoder.Decode(&candidate) != nil {
		return errors.New("invalid project configuration")
	}
	if _, e = readInstallConfigFromStruct(candidate); e != nil {
		return e
	}
	f, e := root.OpenRegular(filepath.Base(path))
	if e != nil {
		return e
	}
	info, e := f.Stat()
	f.Close()
	if e != nil {
		return e
	}
	return root.ReplaceRegular(filepath.Base(path), raw, info.Mode().Perm(), 0700, time.Now())
}
func checkBackendTransition(ctx context.Context, cfg installConfig, owner string) error {
	if _, e := os.Lstat(cfg.DBPath); os.IsNotExist(e) {
		return nil
	} else if e != nil {
		return e
	}
	root, e := securefs.OpenRoot(filepath.Dir(cfg.DBPath))
	if e != nil {
		return e
	}
	f, e := root.OpenRegular(filepath.Base(cfg.DBPath))
	root.Close()
	if e != nil {
		return e
	}
	f.Close()
	u := url.URL{Scheme: "file", Path: cfg.DBPath}
	q := u.Query()
	q.Set("mode", "rw")
	u.RawQuery = q.Encode()
	db, e := sql.Open("sqlite", u.String())
	if e != nil {
		return e
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = providerrequest.RequireHostLocalMode(ctx, tx); e != nil {
		return e
	}
	project, e := access.ProjectIncarnation(ctx, tx)
	if e != nil {
		return e
	}
	return providerrequest.BindLocalOwnerTx(ctx, tx, project, owner)
}
func checkSetupChatGPT(ctx context.Context, c localstack.Config) error {
	if c.Model.AuthKind != "chatgpt" {
		return nil
	}
	key, e := readChatGPTKey(c.MasterKeyFile, c.OwnerID)
	if e != nil {
		return e
	}
	defer clear(key)
	root, e := securefs.OpenRoot(c.StateRoot)
	if e != nil {
		return e
	}
	f, e := root.OpenRegular("chatgpt.db")
	root.Close()
	if e != nil {
		return e
	}
	e = securefs.ValidateCurrentUserOnly(f)
	f.Close()
	if e != nil {
		return e
	}
	u := url.URL{Scheme: "file", Path: filepath.Join(c.StateRoot, "chatgpt.db")}
	q := u.Query()
	q.Set("mode", "ro")
	q.Set("_pragma", "query_only(1)")
	u.RawQuery = q.Encode()
	db, e := sql.Open("sqlite", u.String())
	if e != nil {
		return e
	}
	defer db.Close()
	return chatgpt.CheckRegistration(ctx, db, key, c.OwnerID, c.Model.RegistrationID)
}

func syncSetupParent(path string) error {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer root.Close()
	return securefs.SyncDirectory(root)
}
