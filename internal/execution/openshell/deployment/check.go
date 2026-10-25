package deployment

import (
	"context"
	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"kingdom/internal/execution/openshell/localstack"
	"kingdom/internal/execution/openshell/worker"
)

type CheckResult struct {
	BundleVerified        bool `json:"bundle_verified"`
	GatewayAuthenticated  bool `json:"gateway_authenticated"`
	DockerDriverAvailable bool `json:"docker_driver_available"`
	ExecutionAttested     bool `json:"execution_attested"`
}

// Check authenticates read-only gateway metadata. Existing public APIs do not
// attest the managed deployment or worker; normal allocation enforces those pins.
func Check(ctx context.Context, b Bundle) (result CheckResult, err error) {
	if err = b.Validate(); err != nil {
		return result, err
	}
	if err = localstack.CheckTLS(b.LocalConfig()); err != nil {
		return result, err
	}
	result.BundleVerified = true
	g, err := worker.Dial(worker.Endpoint{Address: b.Gateway.Address, CAFile: b.Gateway.CAFile, CertificateFile: b.Gateway.CertificateFile, KeyFile: b.Gateway.KeyFile})
	if err != nil {
		return result, err
	}
	defer g.Close()
	health, err := g.Native.Health(ctx, &native.HealthRequest{})
	if err != nil || health.Status != native.ServiceStatus_SERVICE_STATUS_HEALTHY {
		return result, ErrInvalid
	}
	info, err := g.Native.GetGatewayInfo(ctx, &native.GetGatewayInfoRequest{})
	if err != nil {
		return result, ErrInvalid
	}
	for _, driver := range info.ComputeDrivers {
		if driver.Name == "docker" {
			result.DockerDriverAvailable = true
		}
	}
	if !result.DockerDriverAvailable {
		return result, ErrInvalid
	}
	result.GatewayAuthenticated = true
	return result, nil
}
