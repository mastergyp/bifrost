package managedadmission

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/valyala/fasthttp"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/admission"
	"github.com/maximhq/bifrost/plugins/governance"
)

func profileOwnedToken(api string, body []byte, session string) string {
	digest, _ := admission.RequestDigest(admission.API(api), body)
	now := time.Now().UnixMilli()
	var wire map[string]json.RawMessage
	json.Unmarshal(body, &wire)
	controls := map[string]json.RawMessage{}
	for _, key := range []string{"max_tokens", "max_completion_tokens", "temperature"} {
		if value, ok := wire[key]; ok {
			controls[key] = value
		}
	}
	var sessionHash *string
	if session != "" {
		sum := sha256.Sum256([]byte(session))
		value := fmt.Sprintf("%x", sum)
		sessionHash = &value
	}
	claims := map[string]any{"version": 1, "key_id": "owned-profile-1", "root_id": uuid.NewString(), "call_id": uuid.NewString(), "subject_hash": strings.Repeat("a", 64), "session_hash": sessionHash, "source_alias": "sw-qwen", "allowed_aliases": []string{"sw-glm", "sw-qwen"}, "purpose": "main", "api": api, "body_hash": digest, "issued_at_ms": now, "expires_at_ms": now + 60000, "queue_deadline_ms": now + 45000, "weight": 1, "max_context_tokens": 512, "max_output_tokens": 128, "original_controls": controls}
	raw, _ := json.Marshal(claims)
	mac := hmac.New(sha256.New, []byte(strings.Repeat("k", 32)))
	mac.Write([]byte("sw-gateway-delegation-v1\x00"))
	mac.Write(raw)
	return "swgd1." + base64.RawURLEncoding.EncodeToString(raw) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func ownedConfiguredProfileFixture(t *testing.T) *fixture {
	t.Helper()
	socket := os.Getenv("SW_PROFILE_OWNED_SOCKET")
	if socket == "" {
		t.Skip("owned actual Rust profile only")
	}
	namespace, contract := os.Getenv("SW_PROFILE_NAMESPACE"), os.Getenv("SW_PROFILE_CONTRACT")
	config := admission.Profile{Version: 1, Namespace: namespace, ContractSHA256: contract, Socket: socket, SubjectKeyEnv: "BIFROST_MANAGED_SUBJECT", DelegationKeys: []admission.ProfileDelegationKey{{ID: "owned-profile-1", SecretEnv: "BIFROST_MANAGED_SIGNING", GatewayVirtualKeyIDs: []string{vkID}}}, Models: []admission.ProfileModel{{Alias: "sw-qwen", Pool: "qwen", Provider: schemas.OpenAI, Model: model}, {Alias: "sw-glm", Pool: "glm", Provider: schemas.OpenAI, Model: "gpt-4.1"}}, MaxOperations: 8}
	raw, _ := json.Marshal(config)
	file := filepath.Join(t.TempDir(), "profile.json")
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	factory := func(plugin *governance.GovernancePlugin) *admission.Runtime {
		runtime, err := admission.LoadProfile(context.Background(), file, func(name string) (string, bool) {
			switch name {
			case "BIFROST_MANAGED_SUBJECT":
				return strings.Repeat("s", 32), true
			case "BIFROST_MANAGED_SIGNING":
				return strings.Repeat("k", 32), true
			}
			return "", false
		}, func(ctx *schemas.BifrostContext, typ schemas.RequestType, route schemas.Route, deadline time.Time) (admission.SelectedFunding, *schemas.BifrostError) {
			approval, denied := plugin.CheckManagedTarget(ctx, typ, route, deadline)
			if denied != nil {
				return nil, denied
			}
			return approval, nil
		}, bifrost.NewNoOpLogger())
		if err != nil {
			t.Fatal(err)
		}
		return runtime
	}
	return fixtureWithInner(t, false, true, nil, "qwen", fixtureRouting{keylessEmpty: true, aliases: []admission.NativeModelAlias{{Provider: schemas.OpenAI, Model: "gpt-4.1", SourceAlias: "sw-glm"}}, runtimeFactory: factory})
}

// Explicitly launched by one disposable Rust/PostgreSQL fixture; it never
// discovers production URLs, credentials, model endpoints or databases.
func TestOwnedConfiguredProfileThroughActualRustPlanner(t *testing.T) {
	socket := os.Getenv("SW_PROFILE_OWNED_SOCKET")
	if socket == "" {
		t.Skip("requires owned Rust planner/controller/sender")
	}
	expectedPool := os.Getenv("SW_PROFILE_EXPECT_POOL")
	if expectedPool != "qwen" && expectedPool != "glm" {
		t.Fatal("owned expected pool missing")
	}
	targetModel := model
	if expectedPool == "glm" {
		targetModel = "gpt-4.1"
	}
	for _, gateway := range []bool{false, true} {
		for _, streaming := range []bool{false, true} {
			t.Run(fmt.Sprintf("gateway=%t/stream=%t/pool=%s", gateway, streaming, expectedPool), func(t *testing.T) {
				f := ownedConfiguredProfileFixture(t)
				base, client := ownedHTTP(t, f)
				body, _ := json.Marshal(map[string]any{"model": "openai/" + model, "messages": []any{map[string]any{"role": "user", "content": "owned synthetic prompt"}}, "max_completion_tokens": 17, "stream": streaming})
				path := "/openai" + string(admission.Chat)
				session := ""
				if expectedPool == "glm" {
					session = uuid.NewString()
				}
				if gateway {
					path = admission.GatewayPrefix + string(admission.Chat)
				}
				req, _ := http.NewRequest("POST", base+path, bytes.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+vkValue)
				req.Header.Set("Content-Type", "application/json")
				if session != "" {
					req.Header.Set("X-Session-ID", session)
				}
				if gateway {
					req.Header.Set(admission.DelegationHeader, profileOwnedToken(string(admission.Chat), body, session))
				}
				response, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				if response.StatusCode != 200 {
					bad, _ := io.ReadAll(response.Body)
					response.Body.Close()
					t.Fatalf("status=%d body=%s", response.StatusCode, bad)
				}
				if streaming {
					_, err = io.ReadFull(response.Body, make([]byte, 1))
				} else {
					_, err = io.Copy(io.Discard, response.Body)
				}
				response.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				defer f.client.Shutdown()
				if streaming {
					f.client.Shutdown()
				}
				data := f.store.GetGovernanceData(context.Background())
				rate := data.RateLimits["original-rate"]
				expected := f.catalog.CalculateCost(&schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{Model: targetModel, Usage: &schemas.BifrostLLMUsage{PromptTokens: 5, CompletionTokens: 2, TotalTokens: 7}, ExtraFields: schemas.BifrostResponseExtraFields{RequestType: schemas.ChatCompletionRequest, Provider: schemas.OpenAI, OriginalModelRequested: model, ResolvedModelUsed: targetModel}}}, nil)
				if expected <= 0 || rate.TokenCurrentUsage != 44 || rate.RequestCurrentUsage != 5 || math.Abs(data.Budgets["original-budget"].CurrentUsage-(13.25+expected)) > 1e-9 {
					t.Fatalf("original quota/actual target cost mismatch: tokens=%d requests=%d delta=%v expected=%v", rate.TokenCurrentUsage, rate.RequestCurrentUsage, data.Budgets["original-budget"].CurrentUsage-13.25, expected)
				}
				if f.account.keys.Load() != 0 {
					t.Fatal("profile nested into provider worker/key selection")
				}
				if gateway && !streaming {
					invalid, _ := json.Marshal(map[string]any{"model": "openai/" + model, "messages": []any{map[string]any{"role": "user", "content": "owned invalid prompt"}}, "max_completion_tokens": 17})
					proof := profileOwnedToken(string(admission.Chat), invalid, "")
					for attempt := 0; attempt < 2; attempt++ {
						retry, _ := http.NewRequest("POST", base+path, bytes.NewReader(invalid))
						retry.Header.Set("Authorization", "Bearer "+vkValue)
						retry.Header.Set("Content-Type", "application/json")
						retry.Header.Set(admission.DelegationHeader, proof)
						denied, err := client.Do(retry)
						if err != nil {
							t.Fatal(err)
						}
						io.Copy(io.Discard, denied.Body)
						denied.Body.Close()
						if denied.StatusCode < 400 || !strings.HasPrefix(denied.Header.Get(admission.NoDispatchHeader), "swnd1.") {
							t.Fatalf("pre-ticket rejection missing durable receipt, attempt=%d status=%d", attempt, denied.StatusCode)
						}
					}
					after := f.store.GetGovernanceData(context.Background())
					if after.RateLimits["original-rate"].TokenCurrentUsage != 44 || math.Abs(after.Budgets["original-budget"].CurrentUsage-(13.25+expected)) > 1e-9 {
						t.Fatal("pre-ticket failure charged model tokens or cost")
					}
				}
			})
		}
	}
}

// Test-only server for the real Rust Gateway frontend. It exposes no external
// network or real credentials and exits on the parent's exact stdin signal.
func TestOwnedConfiguredProfileHTTPServerForGateway(t *testing.T) {
	if os.Getenv("SW_PROFILE_GATEWAY_HTTP_SERVER") != "1" {
		t.Skip("owned Gateway joint fixture only")
	}
	f := ownedConfiguredProfileFixture(t)
	var mu sync.Mutex
	var claims []map[string]any
	base, _ := ownedHTTP(t, f, func(ctx *fasthttp.RequestCtx) {
		raw := string(ctx.Request.Header.Peek(admission.DelegationHeader))
		parts := strings.Split(raw, ".")
		if len(parts) != 3 {
			return
		}
		decoded, e := base64.RawURLEncoding.DecodeString(parts[1])
		if e != nil {
			return
		}
		var c map[string]any
		if json.Unmarshal(decoded, &c) != nil {
			return
		}
		safe := map[string]any{"root_id": c["root_id"], "call_id": c["call_id"], "purpose": c["purpose"], "subject_hash": c["subject_hash"], "session_hash": c["session_hash"], "source_alias": c["source_alias"], "allowed_aliases": c["allowed_aliases"], "status": ctx.Response.StatusCode()}
		mu.Lock()
		claims = append(claims, safe)
		mu.Unlock()
	})
	fmt.Println("OWNED_PLANNED_GATEWAY_READY " + base)
	done := make(chan struct{})
	go func() { _, _ = io.ReadFull(os.Stdin, make([]byte, 1)); close(done) }()
	select {
	case <-done:
	case <-time.After(40 * time.Second):
		t.Fatal("parent did not stop owned frontend")
	}
	f.client.Shutdown()
	data := f.store.GetGovernanceData(context.Background())
	cost := func(target string) float64 {
		return f.catalog.CalculateCost(&schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{Model: target, Usage: &schemas.BifrostLLMUsage{PromptTokens: 5, CompletionTokens: 2, TotalTokens: 7}, ExtraFields: schemas.BifrostResponseExtraFields{RequestType: schemas.ChatCompletionRequest, Provider: schemas.OpenAI, OriginalModelRequested: model, ResolvedModelUsed: target}}}, nil)
	}
	expected := 5*cost(model) + 3*cost("gpt-4.1")
	if expected <= 0 || data.RateLimits["original-rate"].TokenCurrentUsage != 93 || data.RateLimits["original-rate"].RequestCurrentUsage != 12 || math.Abs(data.Budgets["original-budget"].CurrentUsage-(13.25+expected)) > 1e-9 {
		t.Fatal("original governance lost actual Gateway/auxiliary target usage")
	}
	if f.account.keys.Load() != 0 {
		t.Fatal("managed Gateway nested provider worker")
	}
	mu.Lock()
	captured := append([]map[string]any{}, claims...)
	mu.Unlock()
	if len(captured) != 8 {
		t.Fatalf("expected 8 actual calls, got %d", len(captured))
	}
	evidence := map[string]any{"passed": true, "actual_gateway_model_calls": 8, "original_governance_tokens_delta": 56, "original_governance_requests_delta": 8, "original_governance_budget_delta": expected, "captured_claims": captured}
	raw, _ := json.MarshalIndent(evidence, "", "  ")
	if err := os.WriteFile(os.Getenv("SW_PROFILE_GATEWAY_EVIDENCE"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}
