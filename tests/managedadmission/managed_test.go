// Real core + Grant resolver + governance counters, synthetic managed producer.
// No provider, SSH, production key/database or external HTTP call is permitted.
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
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fasthttp/router"
	"github.com/google/uuid"
	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/admission"
	"github.com/maximhq/bifrost/framework/configstore"
	tables "github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/framework/grant"
	"github.com/maximhq/bifrost/framework/modelcatalog"
	"github.com/maximhq/bifrost/framework/modelcatalog/datasheet"
	"github.com/maximhq/bifrost/plugins/governance"
	"github.com/maximhq/bifrost/transports/bifrost-http/integrations"
	"github.com/maximhq/bifrost/transports/bifrost-http/lib"
	httpserver "github.com/maximhq/bifrost/transports/bifrost-http/server"
	"github.com/valyala/fasthttp"
)

const model = "gpt-4o" // committed pricing fixture, not a real inference target
const vkID = "owned-native-vk"
const vkValue = "owned-test-only-credential"

type account struct{ keys atomic.Int64 }

func (a *account) GetConfiguredProviders() ([]schemas.ModelProvider, error) {
	return []schemas.ModelProvider{schemas.OpenAI}, nil
}
func (a *account) GetKeysForProvider(context.Context, schemas.ModelProvider) ([]schemas.Key, error) {
	a.keys.Add(1)
	return nil, nil
}
func (a *account) GetConfigForProvider(schemas.ModelProvider) (*schemas.ProviderConfig, error) {
	return &schemas.ProviderConfig{NetworkConfig: schemas.NetworkConfig{BaseURL: "http://127.0.0.1:9", DefaultRequestTimeoutInSeconds: 1}, ConcurrencyAndBufferSize: schemas.ConcurrencyAndBufferSize{Concurrency: 1, BufferSize: 1}}, nil
}

type managed struct {
	authorizer    *admission.NativeAuthorizer
	calls         atomic.Int64
	mu            sync.Mutex
	authorization admission.NativeAuthorization
	captured      *schemas.BifrostResponse
	release       chan struct{}
	releaseOnce   sync.Once
	closed        atomic.Bool
}

func (m *managed) Owns(p schemas.ModelProvider) bool { return p == schemas.OpenAI }
func (m *managed) Close()                            { m.closed.Store(true) }
func (m *managed) finish()                           { m.releaseOnce.Do(func() { close(m.release) }) }
func (m *managed) Handle(ctx *schemas.BifrostContext, r *schemas.BifrostRequest) *schemas.LLMPluginShortCircuit {
	m.calls.Add(1)
	targets := []admission.ModelTarget{{Pool: "home", Provider: schemas.OpenAI, Model: model}, {Pool: "foreign", Provider: schemas.Anthropic, Model: "not-granted"}}
	auth, err := m.authorizer.Authorize(ctx, targets[0], targets)
	if principal := admission.AdmittedPrincipal(ctx); principal != nil {
		auth, err = principal.Authorization(), nil
	}
	if err != nil {
		return &schemas.LLMPluginShortCircuit{Error: &schemas.BifrostError{StatusCode: new(403), Error: &schemas.ErrorField{Message: "original caller authorization required"}}}
	}
	m.mu.Lock()
	m.authorization = auth
	m.mu.Unlock()
	chat := &schemas.BifrostChatResponse{ID: "synthetic-managed", Model: model, Object: "chat.completion", Choices: []schemas.BifrostResponseChoice{{Index: 0, FinishReason: new("stop"), ChatNonStreamResponseChoice: &schemas.ChatNonStreamResponseChoice{Message: &schemas.ChatMessage{Role: schemas.ChatMessageRoleAssistant, Content: &schemas.ChatMessageContent{ContentStr: new("done")}}}}}, Usage: &schemas.BifrostLLMUsage{PromptTokens: 5, CompletionTokens: 2, TotalTokens: 7}}
	response := &schemas.BifrostResponsesResponse{ID: new("synthetic-managed"), Object: "response", Status: new("completed"), Model: model, Usage: &schemas.ResponsesResponseUsage{InputTokens: 5, OutputTokens: 2, TotalTokens: 7}}
	var captured *schemas.BifrostResponse
	switch r.RequestType {
	case schemas.ChatCompletionRequest:
		captured = &schemas.BifrostResponse{ChatResponse: chat}
	case schemas.ResponsesRequest:
		captured = &schemas.BifrostResponse{ResponsesResponse: response}
	case schemas.ChatCompletionStreamRequest:
		captured = &schemas.BifrostResponse{ChatResponse: chat}
	case schemas.ResponsesStreamRequest:
		captured = &schemas.BifrostResponse{ResponsesStreamResponse: &schemas.BifrostResponsesStreamResponse{Type: schemas.ResponsesStreamResponseTypeCompleted, Response: response}}
	default:
		return &schemas.LLMPluginShortCircuit{Error: &schemas.BifrostError{StatusCode: new(400), Error: &schemas.ErrorField{Message: "unsupported owned inference"}}}
	}
	m.mu.Lock()
	m.captured = captured
	m.mu.Unlock()
	if r.RequestType == schemas.ChatCompletionRequest || r.RequestType == schemas.ResponsesRequest {
		return &schemas.LLMPluginShortCircuit{Response: captured}
	}
	requestType := r.RequestType // r is pooled and released when the SDK returns
	ch := make(chan *schemas.BifrostStreamChunk)
	go func() {
		defer close(ch)
		if requestType == schemas.ChatCompletionStreamRequest {
			ch <- &schemas.BifrostStreamChunk{BifrostChatResponse: &schemas.BifrostChatResponse{Model: model, Choices: []schemas.BifrostResponseChoice{{Index: 0, ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{Delta: &schemas.ChatStreamResponseChoiceDelta{Content: new("begin")}}}}}}
		} else {
			ch <- &schemas.BifrostStreamChunk{BifrostResponsesStreamResponse: &schemas.BifrostResponsesStreamResponse{Type: schemas.ResponsesStreamResponseTypeOutputTextDelta, Delta: new("begin")}}
		}
		<-m.release
		ch <- &schemas.BifrostStreamChunk{FinalForAccounting: true, BifrostChatResponse: captured.ChatResponse, BifrostResponsesStreamResponse: captured.ResponsesStreamResponse}
	}()
	return &schemas.LLMPluginShortCircuit{Stream: ch}
}

type fixture struct {
	client  *bifrost.Bifrost
	store   *governance.LocalGovernanceStore
	catalog *modelcatalog.ModelCatalog
	managed *managed
	account *account
	runtime *admission.Runtime
}

func newFixture(t *testing.T, delegated bool, withRuntime ...bool) *fixture {
	t.Helper()
	logger := bifrost.NewNoOpLogger()
	now := time.Now().UTC()
	pricing, err := filepath.Abs("../../framework/modelcatalog/datasheet/testdata/pricing.json")
	if err != nil {
		t.Fatal(err)
	}
	ds := datasheet.New(nil, logger, datasheet.Config{URL: "file://" + pricing})
	if err = ds.LoadFromURLIntoMemory(context.Background()); err != nil {
		t.Fatal(err)
	}
	catalog := modelcatalog.NewTestCatalogWithDatasheet(ds)
	budget := tables.TableBudget{ID: "original-budget", MaxLimit: 1000, ResetDuration: "1d", LastReset: now, CurrentUsage: 13.25, VirtualKeyID: new(vkID)}
	rate := tables.TableRateLimit{ID: "original-rate", TokenMaxLimit: new(int64(1_000_000)), TokenResetDuration: new("1d"), TokenCurrentUsage: 37, TokenLastReset: now, RequestMaxLimit: new(int64(1000)), RequestResetDuration: new("1d"), RequestCurrentUsage: 4, RequestLastReset: now}
	vk := tables.TableVirtualKey{ID: vkID, Name: "owned fixture", Value: *schemas.NewSecretVar(vkValue), IsActive: new(true), ProviderConfigs: []tables.TableVirtualKeyProviderConfig{{Provider: "openai", AllowedModels: schemas.WhiteList{model}, AllowAllKeys: true}}, Budgets: []tables.TableBudget{budget}, RateLimitID: new(rate.ID), RateLimit: &rate}
	store, err := governance.NewLocalGovernanceStore(context.Background(), logger, nil, &configstore.GovernanceConfig{VirtualKeys: []tables.TableVirtualKey{vk}, Budgets: []tables.TableBudget{budget}, RateLimits: []tables.TableRateLimit{rate}}, catalog, nil)
	if err != nil {
		t.Fatal(err)
	}
	plugin, err := governance.InitFromStore(context.Background(), &governance.Config{IsVkMandatory: new(true)}, logger, store, nil, catalog, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var delegates []string
	if delegated {
		delegates = []string{vkID}
	}
	authorizer, err := admission.NewNativeAuthorizer([]byte(strings.Repeat("s", 32)), delegates)
	if err != nil {
		t.Fatal(err)
	}
	manager := &managed{authorizer: authorizer, release: make(chan struct{})}
	a := &account{}
	var boundary schemas.ManagedInference = manager
	var runtime *admission.Runtime
	if len(withRuntime) > 0 && withRuntime[0] {
		native, _ := admission.NewNativeAuthorizer([]byte(strings.Repeat("s", 32)), nil)
		verifier, e := admission.NewDelegationVerifier([]admission.DelegationKey{{ID: "owned-key-1", Secret: bytes.Repeat([]byte{11}, 32), GatewayVirtualKeyIDs: []string{vkID}}})
		if e != nil {
			t.Fatal(e)
		}
		runtime, e = admission.NewRuntime(manager, native, verifier, []admission.RuntimeModel{{Alias: "qwen", ModelTarget: admission.ModelTarget{Pool: "home", Provider: schemas.OpenAI, Model: model}}}, []string{vkID})
		if e != nil {
			t.Fatal(e)
		}
		boundary = runtime
	}
	client, err := bifrost.Init(context.Background(), schemas.BifrostConfig{Account: a, Logger: logger, LLMPlugins: []schemas.LLMPlugin{plugin}, ModelCatalog: catalog, ManagedInference: boundary})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{client, store, catalog, manager, a, runtime}
	t.Cleanup(func() { manager.finish(); client.Shutdown() })
	return f
}
func ctxFor(value string) (*schemas.BifrostContext, context.CancelFunc) {
	ctx, cancel := schemas.NewBifrostContextWithCancel(context.Background())
	g := grant.New()
	g.SetIdentity(grant.NewIdentity(grant.NewCredential(grant.CredentialVirtualKey, value), nil, nil, nil, nil, nil, nil))
	ctx.SetGrant(g)
	return ctx, cancel
}
func request(name string) *schemas.BifrostChatRequest {
	return &schemas.BifrostChatRequest{Provider: schemas.OpenAI, Model: name, Input: []schemas.ChatMessage{{Role: schemas.ChatMessageRoleUser, Content: &schemas.ChatMessageContent{ContentStr: new("synthetic")}}}, Params: &schemas.ChatParameters{User: new("untrusted-user-must-not-be-identity"), MaxCompletionTokens: new(16)}}
}
func (f *fixture) assertUntouched(t *testing.T) {
	t.Helper()
	data := f.store.GetGovernanceData(context.Background())
	if data.Budgets["original-budget"].CurrentUsage != 13.25 || data.RateLimits["original-rate"].TokenCurrentUsage != 37 || data.RateLimits["original-rate"].RequestCurrentUsage != 4 {
		t.Fatal("denied operation changed original counters")
	}
	if f.account.keys.Load() != 0 {
		t.Fatal("managed request reached a provider worker/key selector")
	}
}

func TestOriginalGovernanceChargesManagedUnaryAndCancelledStreamsOnce(t *testing.T) {
	for _, api := range []string{"chat", "responses", "chat-stream", "responses-stream"} {
		t.Run(api, func(t *testing.T) {
			f := newFixture(t, false)
			ctx, cancel := ctxFor(vkValue)
			defer cancel()
			var err *schemas.BifrostError
			switch api {
			case "chat":
				_, err = f.client.ChatCompletionRequest(ctx, request(model))
			case "responses":
				_, err = f.client.ResponsesRequest(ctx, request(model).ToResponsesRequest())
			default:
				var ch chan *schemas.BifrostStreamChunk
				if api == "chat-stream" {
					ch, err = f.client.ChatCompletionStreamRequest(ctx, request(model))
				} else {
					ch, err = f.client.ResponsesStreamRequest(ctx, request(model).ToResponsesRequest())
				}
				if err == nil {
					select {
					case <-ch:
					case <-time.After(3 * time.Second):
						t.Fatal("first chunk missing")
					}
					cancel()
					f.managed.finish()
				}
			}
			if err != nil {
				t.Fatalf("managed %s failed: %+v", api, err)
			}
			f.client.Shutdown() // joins managed post-hooks and original governance async worker
			data := f.store.GetGovernanceData(context.Background())
			rate := data.RateLimits["original-rate"]
			if rate.TokenCurrentUsage != 44 || rate.RequestCurrentUsage != 5 {
				t.Fatalf("original limits charged incorrectly: tokens=%d requests=%d", rate.TokenCurrentUsage, rate.RequestCurrentUsage)
			}
			f.managed.mu.Lock()
			auth, payload := f.managed.authorization, f.managed.captured
			f.managed.mu.Unlock()
			cost := f.catalog.CalculateCost(payload, nil)
			if cost <= 0 || math.Abs(data.Budgets["original-budget"].CurrentUsage-(13.25+cost)) > 1e-10 {
				t.Fatalf("nonzero original money budget not charged once: cost=%g total=%g", cost, data.Budgets["original-budget"].CurrentUsage)
			}
			if len(auth.Pools) != 1 || auth.Pools[0] != "home" || auth.OriginalLimits == nil || auth.OriginalLimits != ctx.Grant().Limits() {
				t.Fatal("original Grant/target restriction was replaced")
			}
			if f.account.keys.Load() != 0 || f.managed.calls.Load() != 1 {
				t.Fatal("provider queue/key path or managed replay observed")
			}
		})
	}
}
func TestOriginalCredentialAndModelDenialsHappenBeforeManagedBoundary(t *testing.T) {
	for _, kind := range []string{"unknown-key", "unlisted-model", "gateway-delegation"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t, kind == "gateway-delegation")
			value, name := vkValue, model
			if kind == "unknown-key" {
				value = "unknown-owned-fixture-key"
			}
			if kind == "unlisted-model" {
				name = "not-granted"
			}
			ctx, cancel := ctxFor(value)
			defer cancel()
			_, err := f.client.ChatCompletionRequest(ctx, request(name))
			if err == nil {
				t.Fatal("denied request succeeded")
			}
			f.client.Shutdown()
			want := int64(0)
			if kind == "gateway-delegation" {
				want = 1
			}
			if f.managed.calls.Load() != want {
				t.Fatalf("governance/delegation order: %d", f.managed.calls.Load())
			}
			f.assertUntouched(t)
		})
	}
}
func TestExhaustedOriginalBudgetCannotReachManagedInference(t *testing.T) {
	f := newFixture(t, false)
	if err := f.store.ChargeBudgets(context.Background(), []schemas.Limit{{ID: "original-budget"}}, 2000); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := ctxFor(vkValue)
	defer cancel()
	_, err := f.client.ChatCompletionRequest(ctx, request(model))
	if err == nil || err.StatusCode == nil || *err.StatusCode != 402 {
		t.Fatalf("original exhausted budget was bypassed: %+v", err)
	}
	f.client.Shutdown()
	if f.managed.calls.Load() != 0 || f.account.keys.Load() != 0 {
		t.Fatal("exhausted budget started model work")
	}
	data := f.store.GetGovernanceData(context.Background())
	if data.Budgets["original-budget"].CurrentUsage != 2013.25 || data.RateLimits["original-rate"].TokenCurrentUsage != 37 || data.RateLimits["original-rate"].RequestCurrentUsage != 4 {
		t.Fatal("refusal reset or charged original counters")
	}
}

func ownedHTTP(t *testing.T, f *fixture) (string, *http.Client) {
	t.Helper()
	config := &lib.Config{ClientConfig: &configstore.ClientConfig{}, ModelCatalog: f.catalog}
	r := router.New()
	integrations.NewOpenAIRouter(f.client, config, nil, bifrost.NewNoOpLogger()).RegisterRoutes(r)
	server := &httpserver.BifrostHTTPServer{Router: r, ManagedAdmission: f.runtime}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	wire := &fasthttp.Server{Handler: server.ManagedRouterHandler()}
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = wire.Serve(listener) }()
	t.Cleanup(func() {
		f.managed.finish()
		f.client.Shutdown()
		_ = wire.Shutdown()
		_ = listener.Close()
		transport.CloseIdleConnections()
		<-stopped
	})
	return "http://" + listener.Addr().String(), client
}
func ownedHTTPToken(api string, body []byte) string {
	digest, _ := admission.RequestDigest(admission.API(api), body)
	now := time.Now().UnixMilli()
	var wire map[string]json.RawMessage
	_ = json.Unmarshal(body, &wire)
	controls := map[string]json.RawMessage{}
	for _, name := range []string{"max_tokens", "max_output_tokens", "temperature", "reasoning_effort"} {
		if value, ok := wire[name]; ok {
			controls[name] = value
		}
	}
	claims := map[string]any{"version": 1, "key_id": "owned-key-1", "root_id": uuid.NewString(), "call_id": uuid.NewString(), "subject_hash": strings.Repeat("a", 64), "session_hash": nil, "source_alias": "qwen", "allowed_aliases": []string{"qwen"}, "purpose": "main", "api": api, "body_hash": digest, "issued_at_ms": now, "expires_at_ms": now + 60000, "queue_deadline_ms": now + 45000, "weight": 1, "max_context_tokens": 32768, "max_output_tokens": 4096, "original_controls": controls}
	raw, _ := json.Marshal(claims)
	mac := hmac.New(sha256.New, bytes.Repeat([]byte{11}, 32))
	mac.Write([]byte("sw-gateway-delegation-v1\x00"))
	mac.Write(raw)
	return "swgd1." + base64.RawURLEncoding.EncodeToString(raw) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
func TestActualHTTPGatewayAndNativeRoutesKeepOriginalAccountingOnUnaryAndCancelledStreams(t *testing.T) {
	for _, api := range []string{string(admission.Chat), string(admission.Responses)} {
		for _, stream := range []bool{false, true} {
			for _, gateway := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%t/gateway=%t", api, stream, gateway), func(t *testing.T) {
					f := newFixture(t, false, true)
					base, client := ownedHTTP(t, f)
					payload := map[string]any{"model": "openai/" + model, "max_tokens": 16, "stream": stream}
					if api == string(admission.Chat) {
						payload["messages"] = []any{map[string]any{"role": "user", "content": "owned synthetic HTTP"}}
					} else {
						delete(payload, "max_tokens")
						payload["max_output_tokens"] = 16
						payload["input"] = "owned synthetic HTTP"
					}
					body, _ := json.Marshal(payload)
					path := "/openai" + api
					if gateway {
						path = admission.GatewayPrefix + api
					}
					request, _ := http.NewRequest("POST", base+path, bytes.NewReader(body))
					request.Header.Set("Content-Type", "application/json")
					request.Header.Set("x-bf-vk", vkValue)
					request.Header.Set("x-request-id", "owned-trace-not-billing-id")
					if gateway {
						request.Header.Set(admission.DelegationHeader, ownedHTTPToken(api, body))
					}
					response, err := client.Do(request)
					if err != nil {
						t.Fatal(err)
					}
					if response.StatusCode != 200 {
						raw, _ := io.ReadAll(response.Body)
						response.Body.Close()
						t.Fatalf("actual Bifrost HTTP status=%d body=%s", response.StatusCode, raw)
					}
					if stream {
						first := make([]byte, 1)
						if _, err = io.ReadFull(response.Body, first); err != nil {
							t.Fatal(err)
						}
						response.Body.Close()
						f.managed.finish()
					} else {
						raw, err := io.ReadAll(response.Body)
						response.Body.Close()
						if err != nil || !bytes.Contains(raw, []byte("synthetic-managed")) {
							t.Fatalf("response lost: %v / %s", err, raw)
						}
					}
					f.client.Shutdown()
					data := f.store.GetGovernanceData(context.Background())
					rate := data.RateLimits["original-rate"]
					f.managed.mu.Lock()
					auth := f.managed.authorization
					f.managed.mu.Unlock()
					// HTTP converters may still populate defaults on their own response.
					// Expected pricing must not read that transport-owned object.
					priced := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{Model: model, Usage: &schemas.BifrostLLMUsage{PromptTokens: 5, CompletionTokens: 2, TotalTokens: 7}}}
					priced.PopulateExtraFields(schemas.ChatCompletionRequest, schemas.OpenAI, model, model)
					cost := f.catalog.CalculateCost(priced, nil)
					if rate.TokenCurrentUsage != 44 || rate.RequestCurrentUsage != 5 || cost <= 0 || math.Abs(data.Budgets["original-budget"].CurrentUsage-(13.25+cost)) > 1e-10 {
						t.Fatalf("real HTTP original accounting changed: tokens=%d requests=%d budget=%g", rate.TokenCurrentUsage, rate.RequestCurrentUsage, data.Budgets["original-budget"].CurrentUsage)
					}
					if len(auth.Pools) != 1 || auth.Pools[0] != "home" || auth.OriginalLimits == nil || f.managed.calls.Load() != 1 || f.account.keys.Load() != 0 {
						t.Fatal("real HTTP scope/owner or managed once boundary changed")
					}
				})
			}
		}
	}
}

func TestActualHTTPGatewayDenialsDoNotSpendOrReachManagedProducer(t *testing.T) {
	for _, kind := range []string{"missing-proof", "altered-body", "native-channel", "source-mismatch", "unknown-key", "disabled-runtime"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t, false, true)
			if kind == "disabled-runtime" {
				f.runtime = nil
			}
			base, client := ownedHTTP(t, f)
			body := []byte(`{"model":"openai/gpt-4o","messages":[{"role":"user","content":"owned"}],"max_tokens":1}`)
			if kind == "source-mismatch" {
				body = []byte(`{"model":"openai/not-granted","messages":[]}`)
			}
			token := ownedHTTPToken(string(admission.Chat), body)
			if kind == "altered-body" {
				body = append(body, ' ')
			}
			path := admission.GatewayPrefix + string(admission.Chat)
			if kind == "native-channel" {
				path = "/openai" + string(admission.Chat)
			}
			request, _ := http.NewRequest("POST", base+path, bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("x-bf-vk", vkValue)
			if kind == "unknown-key" {
				request.Header.Set("x-bf-vk", "owned-invalid-credential")
			}
			if kind != "missing-proof" {
				request.Header.Set(admission.DelegationHeader, token)
			}
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := io.ReadAll(io.LimitReader(response.Body, 65536))
			response.Body.Close()
			want := 401
			if kind == "source-mismatch" {
				want = 403
			}
			if response.StatusCode != want {
				t.Fatalf("denial status=%d want=%d body=%s", response.StatusCode, want, raw)
			}
			f.client.Shutdown()
			f.assertUntouched(t)
			if f.managed.calls.Load() != 0 {
				t.Fatal("denied HTTP request reached managed producer")
			}
		})
	}
}
