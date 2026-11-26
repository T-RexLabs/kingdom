package deployment

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type checkGateway struct {
	native.UnimplementedOpenShellServer
}

func (checkGateway) Health(context.Context, *native.HealthRequest) (*native.HealthResponse, error) {
	return &native.HealthResponse{Status: native.ServiceStatus_SERVICE_STATUS_HEALTHY}, nil
}
func (checkGateway) GetGatewayInfo(context.Context, *native.GetGatewayInfoRequest) (*native.GetGatewayInfoResponse, error) {
	return &native.GetGatewayInfoResponse{ComputeDrivers: []*native.ComputeDriverInfo{{Name: "docker"}}}, nil
}
func TestCheckUsesOnlyAuthenticatedReadOnlyGatewayCalls(t *testing.T) {
	b := fixtureBundle(t)
	dir, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, e := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	keyRaw, _ := x509.MarshalECPrivateKey(key)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyRaw})
	certPath, keyPath := filepath.Join(dir, "cert"), filepath.Join(dir, "key")
	_ = os.WriteFile(certPath, certPEM, 0600)
	_ = os.WriteFile(keyPath, keyPEM, 0600)
	pair, _ := tls.X509KeyPair(certPEM, keyPEM)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	b.Gateway.Address = net.JoinHostPort("localhost", port)
	b.Gateway.CAFile = certPath
	b.Gateway.CertificateFile = certPath
	b.Gateway.KeyFile = keyPath
	b.Callback.CAFile = certPath
	b.Callback.CertificateFile = certPath
	b.Callback.KeyFile = keyPath
	var mu sync.Mutex
	var methods []string
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{pair}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})), grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		mu.Lock()
		methods = append(methods, info.FullMethod)
		mu.Unlock()
		if info.FullMethod != "/openshell.v1.OpenShell/Health" && info.FullMethod != "/openshell.v1.OpenShell/GetGatewayInfo" {
			return nil, status.Error(codes.PermissionDenied, "mutation forbidden")
		}
		return handler(ctx, req)
	}))
	native.RegisterOpenShellServer(server, checkGateway{})
	go server.Serve(listener)
	defer server.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, e := Check(ctx, b)
	if e != nil {
		t.Fatal(e)
	}
	if !result.BundleVerified || !result.GatewayAuthenticated || !result.DockerDriverAvailable || result.ExecutionAttested {
		t.Fatal("false remote attestation", result)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(methods) != 2 {
		t.Fatal("unexpected gateway calls", methods)
	}
}
