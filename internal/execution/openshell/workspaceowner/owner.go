// Package workspaceowner retains native workspace creation identity across
// lost responses. Persistence ownership is supplied by the deployment assembly.
package workspaceowner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"time"
)

type Client interface {
	CreateWorkspace(context.Context, *native.CreateWorkspaceRequest, ...grpc.CallOption) (*native.CreateWorkspaceResponse, error)
	GetWorkspace(context.Context, *native.GetWorkspaceRequest, ...grpc.CallOption) (*native.GetWorkspaceResponse, error)
}
type Pins struct{ Owner, Gateway, Database string }
type Mapping struct{ Name, ID, RequestID, DatabaseID, Dispatched string }
type Repository interface {
	Reserve(context.Context, Pins, Mapping) (Mapping, error)
	MarkDispatched(context.Context, Pins, string) (string, error)
	BindIdentity(context.Context, Pins, string) (string, error)
}

func Ensure(ctx context.Context, repo Repository, client Client, p Pins, now time.Time) (Mapping, error) {
	if repo == nil || client == nil || p.Owner == "" || p.Gateway == "" || p.Database == "" {
		return Mapping{}, errors.New("workspace owner dependencies required")
	}
	request := uuid.NewString()
	hash := sha256.Sum256([]byte(p.Owner + "/" + request))
	m, err := repo.Reserve(ctx, p, Mapping{Name: "kp-" + hex.EncodeToString(hash[:8]), RequestID: request, DatabaseID: p.Database})
	if err != nil {
		return m, err
	}
	if m.DatabaseID != p.Database {
		return m, errors.New("project database identity conflicts with retained workspace")
	}
	if m.ID != "" {
		got, err := client.GetWorkspace(ctx, &native.GetWorkspaceRequest{Name: m.Name})
		if err != nil {
			return m, err
		}
		if got.GetWorkspace().GetMetadata().GetId() != m.ID || got.GetWorkspace().GetMetadata().GetName() != m.Name {
			return m, errors.New("workspace identity changed")
		}
		return m, nil
	}
	if m.Dispatched == "" {
		m.Dispatched, err = repo.MarkDispatched(ctx, p, now.UTC().Format(time.RFC3339Nano))
		if err != nil {
			return m, err
		}
	}
	dispatched, err := time.Parse(time.RFC3339Nano, m.Dispatched)
	if err != nil || now.Before(dispatched) || now.Sub(dispatched) >= 23*time.Hour {
		return m, errors.New("workspace creation remains unresolved beyond replay window")
	}
	got, err := client.CreateWorkspace(ctx, &native.CreateWorkspaceRequest{Name: m.Name, RequestId: m.RequestID})
	if err != nil {
		return m, err
	}
	metadata := got.GetWorkspace().GetMetadata()
	if metadata.GetName() != m.Name || metadata.GetId() == "" {
		return m, errors.New("workspace response identity mismatch")
	}
	m.ID = metadata.GetId()
	saved, err := repo.BindIdentity(ctx, p, m.ID)
	if err != nil {
		return m, err
	}
	if saved != m.ID {
		return m, errors.New("workspace response conflicts with retained identity")
	}
	return m, nil
}
