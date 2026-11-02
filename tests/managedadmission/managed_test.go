// Real core + Grant resolver + governance counters, synthetic managed producer.
// No provider, SSH, production key/database or external HTTP call is permitted.
package managedadmission

import (
	"context"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/admission"
	"github.com/maximhq/bifrost/framework/configstore"
	tables "github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/framework/grant"
	"github.com/maximhq/bifrost/framework/modelcatalog"
	"github.com/maximhq/bifrost/framework/modelcatalog/datasheet"
	"github.com/maximhq/bifrost/plugins/governance"
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
}

func newFixture(t *testing.T, delegated bool) *fixture {
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
	client, err := bifrost.Init(context.Background(), schemas.BifrostConfig{Account: a, Logger: logger, LLMPlugins: []schemas.LLMPlugin{plugin}, ModelCatalog: catalog, ManagedInference: manager})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{client, store, catalog, manager, a}
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
