package hostedbuilder

import "testing"

func TestWorkerOriginIsIndependentOfBrowserOrigin(t *testing.T) {
	c := Config{PublicOrigin: "https://localhost:8443", WorkerOrigin: "https://host.lima.internal:8443"}
	if got := c.workerBase("project"); got != "https://host.lima.internal:8443/worker/projects/project" {
		t.Fatal(got)
	}
	if c.PublicOrigin != "https://localhost:8443" {
		t.Fatal("browser origin changed")
	}
	c.WorkerOrigin = ""
	if got := c.workerBase("project"); got != "https://localhost:8443/worker/projects/project" {
		t.Fatal(got)
	}
}
func TestWorkerOriginsRequireExactTLSOrigin(t *testing.T) {
	for _, raw := range []string{"http://localhost", "https://host/path", "https://user@host", "https://host?", "https://host#", "https://host/?x=1", "https://"} {
		if validOrigin(raw) {
			t.Fatalf("accepted invalid origin %q", raw)
		}
	}
	for _, raw := range []string{"https://localhost:8443", "https://host.lima.internal:8443", "https://[::1]:8443"} {
		if !validOrigin(raw) {
			t.Fatalf("rejected origin %q", raw)
		}
	}
}
