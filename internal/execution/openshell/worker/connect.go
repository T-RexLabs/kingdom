package worker

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"net"
	"os"
	"strings"

	sdk "github.com/NVIDIA/OpenShell/sdk/go/openshell/v1"
	sdktypes "github.com/NVIDIA/OpenShell/sdk/go/openshell/v1/types"
	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/openshell/tunnel"
	"kingdom/internal/execution/remote"
)

// Endpoint is trusted operator configuration. Every client requires mutual TLS;
// there is no insecure mode, system proxy or automatic mutation retry.
type Endpoint struct{ Address, CAFile, CertificateFile, KeyFile string }
type Gateway struct {
	Forwarder tunnel.Forwarder
	Native    native.OpenShellClient
	Managed   pb.ManagedExecutionsClient
	conn      *grpc.ClientConn
	sdk       *sdk.Client
}

func (c *Gateway) Close() error { return errors.Join(c.sdk.Close(), c.conn.Close()) }

type Connected struct {
	*Worker
	*Gateway
}

func Connect(db *sql.DB, requests *remote.RequestStore, c Config, e Endpoint) (*Connected, error) {
	gateway, err := Dial(e)
	if err != nil {
		return nil, err
	}
	w, err := New(db, requests, gateway.Native, gateway.Managed, c)
	if err != nil {
		gateway.Close()
		return nil, err
	}
	return &Connected{w, gateway}, nil
}

// Dial opens only authenticated gateway transports. It does not create resources.
func Dial(e Endpoint) (*Gateway, error) {
	host, _, err := net.SplitHostPort(e.Address)
	if err != nil || host == "" || strings.ContainsAny(e.Address, "/\\\x00\r\n") || e.CAFile == "" || e.CertificateFile == "" || e.KeyFile == "" {
		return nil, errors.New("invalid managed gateway endpoint")
	}
	raw, err := os.ReadFile(e.CAFile)
	if err != nil {
		return nil, errors.New("managed gateway trust unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(raw) {
		return nil, errors.New("invalid managed gateway trust")
	}
	cert, err := tls.LoadX509KeyPair(e.CertificateFile, e.KeyFile)
	if err != nil {
		return nil, errors.New("managed gateway client identity unavailable")
	}
	conn, err := grpc.NewClient(e.Address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13, ServerName: host})), grpc.WithDisableRetry())
	if err != nil {
		return nil, err
	}
	client, err := sdk.NewClient(sdk.Config{Address: e.Address, TLS: &sdk.TLSConfig{CAFile: e.CAFile, CertFile: e.CertificateFile, KeyFile: e.KeyFile}, RetryPolicy: &sdktypes.RetryPolicy{MaxRetries: 0}})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Gateway{Forwarder: client.TCP(), Native: native.NewOpenShellClient(conn), Managed: pb.NewManagedExecutionsClient(conn), conn: conn, sdk: client}, nil
}
