package admission

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
)

const profileNamespace = "d5f6d5c9-a5fb-4bd1-9a0d-7fd4af925480"

func profileSocket(t *testing.T, prepare http.HandlerFunc, policyVersion int, changeHealth ...func(map[string]any)) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "sw-profile-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "control.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/prepare", prepare)
	mux.HandleFunc("/v1/health", func(w http.ResponseWriter, r *http.Request) {
		if rNamespace := r.Header.Get("X-SW-Admission-Namespace"); rNamespace != profileNamespace {
			t.Error("controller request omitted the immutable namespace")
		}
		health := map[string]any{"protocol": 1, "state": "ready", "routing_enforcement": "enabled", "absolute_queue_deadline": true, "preparation_policy_version": policyVersion, "namespace": profileNamespace, "contract_sha256": strings.Repeat("a", 64)}
		for _, change := range changeHealth {
			change(health)
		}
		json.NewEncoder(w).Encode(health)
	})
	server := &http.Server{Handler: mux}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	return path
}
func profileConfig(socket string) Profile {
	return Profile{Version: 1, Namespace: profileNamespace, ContractSHA256: strings.Repeat("a", 64), Socket: socket, SubjectKeyEnv: "BIFROST_MANAGED_SUBJECT", DelegationKeys: []ProfileDelegationKey{{ID: "key-1", SecretEnv: "BIFROST_MANAGED_SIGNING", GatewayVirtualKeyIDs: []string{"gateway-vk-id"}}}, Models: []ProfileModel{{Alias: "sw-qwen", Pool: "qwen", Provider: schemas.OpenAI, Model: "native-qwen"}}, MaxOperations: 8}
}
func writeProfile(t *testing.T, config Profile) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "profile.json")
	raw, _ := json.Marshal(config)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func profileSecrets(name string) (string, bool) {
	switch name {
	case "BIFROST_MANAGED_SUBJECT":
		return strings.Repeat("s", 32), true
	case "BIFROST_MANAGED_SIGNING":
		return strings.Repeat("k", 32), true
	}
	return "", false
}
func profileFunding(*schemas.BifrostContext, schemas.RequestType, schemas.Route, time.Time) (SelectedFunding, *schemas.BifrostError) {
	return nil, producerError(403, "fixture denies all model IO").Error
}

func TestManagedProfileLoadsSameRuntimeAndPreservesRPCBody(t *testing.T) {
	raw := []byte(`{"messages":[{"role":"user","content":"<html>&content</html>"}],"model":"native-qwen"}`)
	calls := 0
	socket := profileSocket(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("X-SW-Admission-Namespace") != profileNamespace || r.Header.Get("X-SW-Admission-Contract") != strings.Repeat("a", 64) {
			t.Error("fixed controller authority missing from preparation request")
		}
		var request struct {
			Protocol     int             `json:"protocol"`
			API          API             `json:"api"`
			Alias        string          `json:"alias"`
			AllowedPools []string        `json:"allowed_pools"`
			MaxContext   uint32          `json:"max_context_tokens"`
			MaxOutput    uint32          `json:"max_output_tokens"`
			Body         json.RawMessage `json:"body"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Protocol != 1 || request.API != Chat || request.Alias != "sw-qwen" || string(request.Body) != string(raw) || request.MaxContext != 200 || request.MaxOutput != 50 {
			t.Error("original bounded preparation metadata/body changed")
		}
		json.NewEncoder(w).Encode(map[string]any{"protocol": 1, "namespace": profileNamespace, "contract_sha256": strings.Repeat("a", 64), "pools": []string{"qwen"}, "healthy_engines": []string{"qwen-1"}, "cost": 70, "native_weight": 1000})
	}, 1)
	runtime, err := LoadProfile(context.Background(), writeProfile(t, profileConfig(socket)), profileSecrets, profileFunding, bifrost.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if !runtime.Owns(schemas.OpenAI) {
		t.Fatal("configured provider was not installed")
	}
	principal := &Principal{source: runtime.models[0], authorization: NativeAuthorization{Pools: []string{"qwen"}}, delegation: &VerifiedDelegation{claims: delegationClaims{MaxContextTokens: 200, MaxOutputTokens: 50}}}
	policy := runtime.inner.(*Producer).policy
	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Second))
	defer ctx.Cancel()
	plan, err := policy.Prepare(ctx, FrozenInput{api: Chat, body: raw}, principal)
	if err != nil || calls != 1 || plan.Cost != 70 || plan.Pools[0] != "qwen" {
		t.Fatalf("original plan not preserved: %v", err)
	}
	selected, denied := policy.(SelectedTargetPolicy).AuthorizeSelected(ctx, principal, Lease{Pool: "qwen"}, schemas.ChatCompletionRequest)
	if denied == nil || selected.Funding != nil {
		t.Fatal("original target funding denial bypassed")
	}
}
func TestManagedProfileRefusesControllerWithoutPreparation(t *testing.T) {
	socket := profileSocket(t, func(w http.ResponseWriter, r *http.Request) { t.Error("old controller must not receive preparation") }, 0)
	_, err := LoadProfile(context.Background(), writeProfile(t, profileConfig(socket)), profileSecrets, profileFunding, bifrost.NewNoOpLogger())
	if !errors.Is(err, ErrNotEnforced) {
		t.Fatalf("legacy controller allowed: %v", err)
	}
}
func TestManagedProfileRefusesDifferentControllerAuthority(t *testing.T) {
	for _, field := range []string{"namespace", "contract_sha256"} {
		t.Run(field, func(t *testing.T) {
			socket := profileSocket(t, func(w http.ResponseWriter, r *http.Request) { t.Error("mismatched controller reached preparation") }, 1, func(health map[string]any) { health[field] = "different" })
			_, err := LoadProfile(context.Background(), writeProfile(t, profileConfig(socket)), profileSecrets, profileFunding, bifrost.NewNoOpLogger())
			if !errors.Is(err, ErrNotEnforced) {
				t.Fatalf("different controller contract accepted: %v", err)
			}
		})
	}
}

func TestManagedProfileFileAndSecretBoundaries(t *testing.T) {
	socket := profileSocket(t, func(w http.ResponseWriter, r *http.Request) { t.Error("invalid config reached request execution") }, 1)
	for _, kind := range []string{"world-write", "symlink", "duplicate", "unknown", "trailing", "oversized", "secret-name", "missing-secret", "reused-secret", "provider-conflict", "duplicate-key-id", "zero-operations"} {
		t.Run(kind, func(t *testing.T) {
			config := profileConfig(socket)
			lookup := profileSecrets
			if kind == "secret-name" {
				config.SubjectKeyEnv = "LLM_UPSTREAM_API_KEY"
			}
			if kind == "missing-secret" {
				lookup = func(string) (string, bool) { return "", false }
			}
			if kind == "reused-secret" {
				lookup = func(string) (string, bool) { return strings.Repeat("s", 32), true }
			}
			if kind == "provider-conflict" {
				config.Models = append(config.Models, ProfileModel{Alias: "other", Pool: "qwen", Provider: schemas.OpenAI, Model: "different"})
			}
			if kind == "duplicate-key-id" {
				config.DelegationKeys = append(config.DelegationKeys, config.DelegationKeys[0])
			}
			if kind == "zero-operations" {
				config.MaxOperations = 0
			}
			path := writeProfile(t, config)
			switch kind {
			case "world-write":
				os.Chmod(path, 0666)
			case "symlink":
				link := path + ".link"
				os.Symlink(path, link)
				path = link
			case "duplicate":
				os.WriteFile(path, []byte(`{"version":1,"version":1}`), 0600)
			case "unknown":
				os.WriteFile(path, []byte(`{"version":1,"secret":"must-not-be-printed"}`), 0600)
			case "trailing":
				f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
				f.WriteString(` {"version":1}`)
				f.Close()
			case "oversized":
				os.WriteFile(path, []byte(strings.Repeat("x", 262145)), 0600)
			}
			runtime, err := LoadProfile(context.Background(), path, lookup, profileFunding, bifrost.NewNoOpLogger())
			if runtime != nil {
				runtime.Close()
			}
			if err == nil {
				t.Fatal("unsafe profile accepted")
			}
			if strings.Contains(err.Error(), "must-not-be-printed") || strings.Contains(err.Error(), strings.Repeat("s", 32)) {
				t.Fatal("private input leaked in error")
			}
		})
	}
}
func TestManagedPreparationRefusesExpandedMalformedAndUnavailablePlans(t *testing.T) {
	for _, kind := range []string{"scope", "duplicate-pool", "duplicate-engine", "unknown-pool", "empty-engines", "zero-cost", "weight", "protocol", "duplicate-json", "trailing-json", "oversized", "unavailable", "rejected", "wrong-namespace", "wrong-contract"} {
		t.Run(kind, func(t *testing.T) {
			socket := profileSocket(t, func(w http.ResponseWriter, r *http.Request) {
				reply := map[string]any{"protocol": 1, "pools": []string{"qwen"}, "healthy_engines": []string{"e1"}, "cost": 1, "native_weight": 1000, "namespace": profileNamespace, "contract_sha256": strings.Repeat("a", 64)}
				switch kind {
				case "wrong-namespace":
					reply["namespace"] = "different"
				case "wrong-contract":
					reply["contract_sha256"] = strings.Repeat("b", 64)
				case "scope":
					reply["pools"] = []string{"glm"}
				case "duplicate-pool":
					reply["pools"] = []string{"qwen", "qwen"}
				case "duplicate-engine":
					reply["healthy_engines"] = []string{"e1", "e1"}
				case "unknown-pool":
					reply["pools"] = []string{"missing"}
				case "empty-engines":
					reply["healthy_engines"] = []string{}
				case "zero-cost":
					reply["cost"] = 0
				case "weight":
					reply["native_weight"] = 1001
				case "protocol":
					reply["protocol"] = 2
				case "duplicate-json":
					io.WriteString(w, `{"protocol":1,"protocol":1}`)
					return
				case "trailing-json":
					json.NewEncoder(w).Encode(reply)
					io.WriteString(w, `{}`)
					return
				case "oversized":
					io.WriteString(w, strings.Repeat("x", 16385))
					return
				case "unavailable":
					w.WriteHeader(503)
					return
				case "rejected":
					w.WriteHeader(422)
					return
				}
				json.NewEncoder(w).Encode(reply)
			}, 1)
			client, err := New(socket)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			client.authority = &controlAuthority{namespace: profileNamespace, contract: strings.Repeat("a", 64)}
			policy := &preparationPolicy{client: client, targets: map[string]ModelTarget{"qwen": {Pool: "qwen"}, "glm": {Pool: "glm"}}, funding: profileFunding}
			ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Second))
			defer ctx.Cancel()
			_, err = policy.Prepare(ctx, FrozenInput{api: Chat, body: []byte(`{"messages":[]}`)}, &Principal{source: RuntimeModel{Alias: "sw-qwen"}, authorization: NativeAuthorization{Pools: []string{"qwen", "missing"}}})
			if err == nil {
				t.Fatal("unsafe preparation accepted")
			}
			if kind == "unavailable" && !errors.Is(err, ErrUnavailable) {
				t.Fatal("service failure was mislabeled caller error")
			}
		})
	}
}
