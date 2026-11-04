package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/admission"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/plugins/governance"
	"github.com/maximhq/bifrost/transports/bifrost-http/lib"
)

func TestManagedStartupLeavesUnconfiguredTransportNative(t *testing.T) {
	previous, present := os.LookupEnv("BIFROST_MANAGED_ADMISSION_CONFIG")
	os.Unsetenv("BIFROST_MANAGED_ADMISSION_CONFIG")
	t.Cleanup(func() {
		if present {
			os.Setenv("BIFROST_MANAGED_ADMISSION_CONFIG", previous)
		}
	})
	server := &BifrostHTTPServer{}
	if err := server.configureManagedAdmission(context.Background()); err != nil || server.managedInferenceBoundary() != nil {
		t.Fatal("unset managed profile changed native transport")
	}
}
func TestManagedStartupRequiresOriginalGovernanceAndExplicitProfile(t *testing.T) {
	t.Setenv("BIFROST_MANAGED_ADMISSION_CONFIG", "")
	server := &BifrostHTTPServer{}
	if server.configureManagedAdmission(context.Background()) == nil {
		t.Fatal("explicit empty profile silently ignored")
	}
	t.Setenv("BIFROST_MANAGED_ADMISSION_CONFIG", "/does-not-exist")
	server.Config = &lib.Config{}
	server.Ctx = schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	if server.configureManagedAdmission(context.Background()) == nil {
		t.Fatal("missing governance/profile accepted")
	}
	if server.ManagedAdmission != nil {
		t.Fatal("failed initialization installed a runtime")
	}
}
func TestManagedStartupInstallsProfileAfterOriginalGovernance(t *testing.T) {
	oldLogger := logger
	SetLogger(bifrost.NewNoOpLogger())
	t.Cleanup(func() { SetLogger(oldLogger) })
	dir, err := os.MkdirTemp("/tmp", "sw-profile-start-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "control.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/health" {
			t.Error("startup sent an inference/preparation request")
			w.WriteHeader(503)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"protocol": 1, "state": "ready", "routing_enforcement": "enabled", "absolute_queue_deadline": true, "preparation_policy_version": 1, "durable_rejection_version": 1, "namespace": "d5f6d5c9-a5fb-4bd1-9a0d-7fd4af925480", "contract_sha256": strings.Repeat("a", 64)})
	})}
	go httpServer.Serve(listener)
	t.Cleanup(func() { httpServer.Close() })
	profile := map[string]any{"version": 1, "namespace": "d5f6d5c9-a5fb-4bd1-9a0d-7fd4af925480", "contract_sha256": strings.Repeat("a", 64), "socket": socket, "subject_key_env": "BIFROST_MANAGED_SUBJECT", "delegation_keys": []any{map[string]any{"id": "key-1", "secret_env": "BIFROST_MANAGED_SIGNING", "gateway_virtual_key_ids": []string{"gateway-id"}}}, "models": []any{map[string]any{"alias": "sw-qwen", "pool": "qwen", "provider": "openai", "model": "native-qwen"}}, "max_operations": 4}
	raw, _ := json.Marshal(profile)
	path := filepath.Join(dir, "profile.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BIFROST_MANAGED_ADMISSION_CONFIG", path)
	t.Setenv("BIFROST_MANAGED_SUBJECT", strings.Repeat("s", 32))
	t.Setenv("BIFROST_MANAGED_SIGNING", strings.Repeat("k", 32))
	server := &BifrostHTTPServer{Config: &lib.Config{}, Ctx: schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)}
	plugins := []schemas.BasePlugin{&governance.GovernancePlugin{}}
	server.Config.BasePlugins.Store(&plugins)
	if err = server.configureManagedAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer server.ManagedAdmission.Close()
	if server.managedInferenceBoundary() == nil || !server.ManagedAdmission.Owns(schemas.OpenAI) {
		t.Fatal("standalone startup composition not installed")
	}
	if server.configureManagedAdmission(context.Background()) == nil {
		t.Fatal("live runtime was replaced on repeated configuration")
	}
}

type ownedStreamPolicyFunding struct{ bound bool }

func (f *ownedStreamPolicyFunding) Target() schemas.Route {
	return schemas.Route{Provider: "gpu2", Model: "owned-model"}
}
func (f *ownedStreamPolicyFunding) RequestType() schemas.RequestType {
	return schemas.ResponsesStreamRequest
}
func (f *ownedStreamPolicyFunding) Bind(*schemas.BifrostContext) *schemas.BifrostError {
	f.bound = true
	return nil
}
func TestManagedStreamPolicyUsesSelectedProviderAndPreservesFunding(t *testing.T) {
	for _, tc := range []struct {
		seconds int
		want    time.Duration
	}{{120, 120 * time.Second}, {600, 600 * time.Second}, {0, 120 * time.Second}, {-1, 120 * time.Second}, {999999, 20 * time.Minute}} {
		source := configstore.ProviderConfig{NetworkConfig: &schemas.NetworkConfig{StreamIdleTimeoutInSeconds: 1}}
		target := configstore.ProviderConfig{NetworkConfig: &schemas.NetworkConfig{StreamIdleTimeoutInSeconds: tc.seconds}}
		server := &BifrostHTTPServer{Config: &lib.Config{Providers: map[schemas.ModelProvider]configstore.ProviderConfig{"gpu0": source, "gpu2": target}}}
		original := &ownedStreamPolicyFunding{}
		selected, denied := server.withManagedStreamPolicy(original, "gpu2")
		if denied != nil {
			t.Fatal("selected configured policy rejected")
		}
		wrapped := selected.(*managedStreamFunding)
		if wrapped.StreamIdleTimeout() != tc.want || wrapped.SelectedFunding != admission.SelectedFunding(original) || selected.Target() != original.Target() || selected.RequestType() != original.RequestType() {
			t.Fatal("selected policy/funding replaced or source policy used")
		}
		selected.Bind(nil)
		if !original.bound || target.NetworkConfig.StreamIdleTimeoutInSeconds != tc.seconds {
			t.Fatal("original binding/configuration changed")
		}
	}
	server := &BifrostHTTPServer{Config: &lib.Config{Providers: map[schemas.ModelProvider]configstore.ProviderConfig{}}}
	if funding, denied := server.withManagedStreamPolicy(&ownedStreamPolicyFunding{}, "missing"); funding != nil || denied == nil || denied.StatusCode == nil || *denied.StatusCode != 503 {
		t.Fatal("missing selected config silently accepted")
	}
}
