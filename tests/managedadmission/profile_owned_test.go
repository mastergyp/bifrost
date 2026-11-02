package managedadmission

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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

// Explicitly launched by one disposable Rust/PostgreSQL fixture; it never
// discovers production URLs, credentials, model endpoints or databases.
func TestOwnedConfiguredProfileThroughActualRustPlanner(t *testing.T) {
	socket := os.Getenv("SW_PROFILE_OWNED_SOCKET")
	if socket == "" {
		t.Skip("requires owned Rust planner/controller/sender")
	}
	namespace, contract := os.Getenv("SW_PROFILE_NAMESPACE"), os.Getenv("SW_PROFILE_CONTRACT")
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
				f := fixtureWithInner(t, false, true, nil, "qwen", fixtureRouting{keylessEmpty: true, aliases: []admission.NativeModelAlias{{Provider: schemas.OpenAI, Model: "gpt-4.1", SourceAlias: "sw-glm"}}, runtimeFactory: factory})
				base, client := ownedHTTP(t, f)
				body, _ := json.Marshal(map[string]any{"model": "openai/" + model, "messages": []any{map[string]any{"role": "user", "content": "owned synthetic prompt"}}, "max_completion_tokens": 17, "stream": streaming})
				path := string(admission.Chat)
				session := ""
				if expectedPool == "glm" {
					session = uuid.NewString()
				}
				if gateway {
					path = admission.GatewayPrefix + path
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
				f.client.Shutdown()
				data := f.store.GetGovernanceData(context.Background())
				rate := data.RateLimits["original-rate"]
				expected := f.catalog.CalculateCost(&schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{Model: targetModel, Usage: &schemas.BifrostLLMUsage{PromptTokens: 5, CompletionTokens: 2, TotalTokens: 7}, ExtraFields: schemas.BifrostResponseExtraFields{RequestType: schemas.ChatCompletionRequest, Provider: schemas.OpenAI, OriginalModelRequested: model, ResolvedModelUsed: targetModel}}}, nil)
				if expected <= 0 || rate.TokenCurrentUsage != 44 || rate.RequestCurrentUsage != 5 || math.Abs(data.Budgets["original-budget"].CurrentUsage-(13.25+expected)) > 1e-9 {
					t.Fatalf("original quota/actual target cost mismatch: tokens=%d requests=%d delta=%v expected=%v", rate.TokenCurrentUsage, rate.RequestCurrentUsage, data.Budgets["original-budget"].CurrentUsage-13.25, expected)
				}
				if f.account.keys.Load() != 0 {
					t.Fatal("profile nested into provider worker/key selection")
				}
			})
		}
	}
}
