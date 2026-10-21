// Package workspacetransfer defines an invocation-owned, one-shot workspace
// transfer. HTTP delivery is not execution closure or publication authority.
package workspacetransfer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/workspacearchive"
)

const HelperPath = "/usr/local/libexec/kingdom-workspace-helper"
const WorkspaceParent = "/workspace"
const ProjectPath = "/workspace/project"
const Port uint32 = 4097
const MaxJobBytes = 16 << 10
const MaxCompletionBytes = 4096

type Job struct {
	Version        int                     `json:"version"`
	Mode           string                  `json:"mode"`
	Nonce          string                  `json:"nonce"`
	HelperSHA256   string                  `json:"helper_sha256"`
	Limits         workspacearchive.Limits `json:"limits"`
	TimeoutSeconds int64                   `json:"timeout_seconds"`
	ArchiveSHA256  string                  `json:"archive_sha256,omitempty"`
	ArchiveBytes   int64                   `json:"archive_bytes,omitempty"`
}

// Never include the one-use capability in formatted diagnostics.
func (Job) String() string   { return "workspace transfer job" }
func (Job) GoString() string { return "workspace transfer job" }

type Completion struct {
	Version        int    `json:"version"`
	Mode           string `json:"mode"`
	ArchiveSHA256  string `json:"archive_sha256"`
	ArchiveBytes   int64  `json:"archive_bytes"`
	ManifestSHA256 string `json:"manifest_sha256"`
	ExpandedBytes  int64  `json:"expanded_bytes"`
	Entries        int    `json:"entries"`
}

func DefaultLimits() workspacearchive.Limits {
	return workspacearchive.Limits{ArchiveBytes: 128 << 20, ExpandedBytes: 1 << 30, FileBytes: 64 << 20, Entries: 32768, Depth: 64, PathBytes: 4096}
}
func digest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && strings.ToLower(value) == value
}
func (j Job) Validate() error {
	max := DefaultLimits()
	l := j.Limits
	if j.Version != 1 || (j.Mode != "import" && j.Mode != "export") || !digest(j.Nonce) || !digest(j.HelperSHA256) || j.TimeoutSeconds < 1 || j.TimeoutSeconds > 3600 || l.ArchiveBytes < 1 || l.ArchiveBytes > max.ArchiveBytes || l.ExpandedBytes < 1 || l.ExpandedBytes > max.ExpandedBytes || l.FileBytes < 1 || l.FileBytes > max.FileBytes || l.FileBytes > l.ExpandedBytes || l.Entries < 1 || l.Entries > max.Entries || l.Depth < 1 || l.Depth > max.Depth || l.PathBytes < 1 || l.PathBytes > max.PathBytes {
		return errors.New("invalid workspace transfer job")
	}
	if j.Mode == "import" {
		if !digest(j.ArchiveSHA256) || j.ArchiveBytes < 1 || j.ArchiveBytes > l.ArchiveBytes {
			return errors.New("invalid workspace import identity")
		}
	} else if j.ArchiveSHA256 != "" || j.ArchiveBytes != 0 {
		return errors.New("export cannot prescribe archive identity")
	}
	return nil
}
func decodeBounded(reader io.Reader, destination any, limit int64) error {
	raw, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return err
	}
	if int64(len(raw)) > limit {
		return errors.New("transfer metadata limit exceeded")
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errors.New("invalid transfer metadata")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("trailing transfer metadata")
	}
	return nil
}
func DecodeJob(reader io.Reader) (Job, error) {
	var job Job
	if err := decodeBounded(reader, &job, MaxJobBytes); err != nil {
		return Job{}, err
	}
	return job, job.Validate()
}

// Command binds all transfer options into the managed command's retained stdin.
// The worker must independently verify the immutable image/helper pin and runtime.
func Command(job Job) (*pb.CommandSpec, error) {
	if err := job.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}
	return &pb.CommandSpec{Argv: []string{HelperPath, "serve-v1"}, Workdir: WorkspaceParent, Stdin: append(raw, '\n'), ExecutionTimeout: durationpb.New(time.Duration(job.TimeoutSeconds+10) * time.Second), OutputLimitBytes: MaxCompletionBytes}, nil
}

// ValidateExport validates retained stdout against a locally staged download.
// Its caller must first validate the exact command receipt, clean closure,
// complete input/output and framed output digest. This function is not that gate.
func ValidateExport(ctx context.Context, parent string, download io.ReaderAt, size int64, stdout io.Reader, job Job) (*workspacearchive.Staged, error) {
	if err := job.Validate(); err != nil || job.Mode != "export" {
		return nil, errors.New("invalid export job")
	}
	var completion Completion
	if err := decodeBounded(stdout, &completion, MaxCompletionBytes); err != nil {
		return nil, err
	}
	if completion.Version != 1 || completion.Mode != "export" || !digest(completion.ArchiveSHA256) || !digest(completion.ManifestSHA256) || completion.ArchiveBytes != size || size < 1 || size > job.Limits.ArchiveBytes {
		return nil, errors.New("invalid export completion")
	}
	staged, err := workspacearchive.ImportZIP(ctx, parent, download, size, job.Limits)
	if err != nil {
		return nil, err
	}
	if completion.ManifestSHA256 != staged.Digest || completion.ArchiveSHA256 != staged.Manifest.ArchiveSHA256 || completion.ExpandedBytes != staged.Manifest.ExpandedBytes || completion.Entries != len(staged.Manifest.Entries) {
		_ = staged.Close()
		return nil, errors.New("workspace export evidence mismatch")
	}
	return staged, nil
}

// ValidateImport compares the helper's retained completion with the exact input
// staged by the host. The caller must separately verify managed closure evidence.
func ValidateImport(stdout io.Reader, job Job, expected *workspacearchive.Staged) error {
	if err := job.Validate(); err != nil || job.Mode != "import" || expected == nil {
		return errors.New("invalid import job")
	}
	var completion Completion
	if err := decodeBounded(stdout, &completion, MaxCompletionBytes); err != nil {
		return err
	}
	if completion.Version != 1 || completion.Mode != "import" || completion.ArchiveSHA256 != job.ArchiveSHA256 || completion.ArchiveBytes != job.ArchiveBytes || completion.ArchiveSHA256 != expected.Manifest.ArchiveSHA256 || completion.ManifestSHA256 != expected.Digest || completion.ExpandedBytes != expected.Manifest.ExpandedBytes || completion.Entries != len(expected.Manifest.Entries) {
		return errors.New("workspace import evidence mismatch")
	}
	return nil
}
