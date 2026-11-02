package managedadmission

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/admission"
	"github.com/maximhq/bifrost/plugins/governance"
)

type fixedPreparation struct {
	pool    string
	engines []string
}

func (p fixedPreparation) Prepare(_ *schemas.BifrostContext, _ admission.FrozenInput, _ *admission.Principal) (admission.Preparation, error) {
	return admission.Preparation{Pools: []string{p.pool}, HealthyEngines: p.engines, Cost: 17, NativeWeight: 7}, nil
}

// Only negative/unit fixtures use this synthetic approval. The actual owned
// HTTP-to-Rust positive fixture below uses original governance-issued funding.
type fixtureFunding struct {
	target schemas.Route
	typ    schemas.RequestType
}

func (f fixtureFunding) RequestType() schemas.RequestType                   { return f.typ }
func (f fixtureFunding) Target() schemas.Route                              { return f.target }
func (f fixtureFunding) Bind(*schemas.BifrostContext) *schemas.BifrostError { return nil }

func (p fixedPreparation) AuthorizeSelected(_ *schemas.BifrostContext, _ *admission.Principal, lease admission.Lease, typ schemas.RequestType) (admission.Selection, *schemas.BifrostError) {
	target := admission.ModelTarget{Pool: lease.Pool, Provider: schemas.OpenAI, Model: model}
	return admission.Selection{Target: target, Funding: fixtureFunding{target: schemas.Route{Provider: target.Provider, Model: target.Model}, typ: typ}}, nil
}

type fundedPreparation struct {
	fixedPreparation
	governance *governance.GovernancePlugin
}

func (p *fundedPreparation) AuthorizeSelected(ctx *schemas.BifrostContext, principal *admission.Principal, lease admission.Lease, typ schemas.RequestType) (admission.Selection, *schemas.BifrostError) {
	selection, _ := p.fixedPreparation.AuthorizeSelected(ctx, principal, lease, typ)
	target := selection.Target
	funding, denied := p.governance.CheckManagedTarget(ctx, typ, schemas.Route{Provider: target.Provider, Model: target.Model}, principal.QueueDeadline())
	if denied != nil {
		return admission.Selection{}, denied
	}
	return admission.Selection{Target: target, Funding: funding}, nil
}

type ownedSocket struct {
	mu       sync.Mutex
	requests map[string]map[string]any
	data     int
	bad      bool
}

func ownedPhysicalSocket(t *testing.T) (*admission.Client, *ownedSocket) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "sw-phys-")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "control.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	state := &ownedSocket{requests: map[string]map[string]any{}}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		write := func(v any) { w.Header().Set("Content-Type", "application/json"); _ = json.NewEncoder(w).Encode(v) }
		switch r.URL.Path {
		case "/v1/health":
			write(map[string]any{"protocol": 1, "state": "ready", "routing_enforcement": "enabled", "absolute_queue_deadline": true})
		case "/v1/enqueue":
			var input map[string]any
			_ = json.NewDecoder(r.Body).Decode(&input)
			req := input["request"].(map[string]any)
			id := req["id"].(string)
			state.mu.Lock()
			if previous, ok := state.requests[id]; ok && previous["request"].(map[string]any)["owner"] != req["owner"] {
				state.mu.Unlock()
				w.WriteHeader(422)
				return
			}
			state.requests[id] = input
			state.mu.Unlock()
			write(map[string]any{"protocol": 1, "ticket": map[string]any{"id": id, "deadline_ms": input["deadline_ms"]}})
		case "/v1/poll":
			var input struct {
				Request admission.Request `json:"request"`
			}
			_ = json.NewDecoder(r.Body).Decode(&input)
			write(map[string]any{"protocol": 1, "decision": map[string]any{"status": "granted", "lease": admission.Lease{ID: input.Request.ID, Pool: "home", Engine: "owned-engine", State: "prepared"}}})
		case "/v1/cancel":
			write(map[string]any{"protocol": 1, "never_dispatched": false})
		case "/inference/v1/chat/completions", "/inference/v1/responses":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			state.mu.Lock()
			state.data++
			if body["temperature"] != 0.25 || body["max_completion_tokens"] != float64(17) && body["max_output_tokens"] != float64(17) {
				state.bad = true
			}
			state.mu.Unlock()
			respondPhysical(w, r.URL.Path, body)
		default:
			w.WriteHeader(404)
		}
	})
	server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()
	client, err := admission.New(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); _ = server.Close(); _ = os.RemoveAll(dir) })
	return client, state
}
func respondPhysical(w http.ResponseWriter, path string, body map[string]any) {
	stream, _ := body["stream"].(bool)
	responses := strings.HasSuffix(path, "/responses")
	response := map[string]any{"id": "owned-physical", "object": "response", "status": "completed", "model": model, "output": []any{}, "usage": map[string]any{"input_tokens": 5, "output_tokens": 2, "total_tokens": 7}}
	chat := map[string]any{"id": "owned-physical", "object": "chat.completion", "model": model, "choices": []any{map[string]any{"index": 0, "finish_reason": "stop", "message": map[string]any{"role": "assistant", "content": "done"}}}, "usage": map[string]any{"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}}
	if !stream {
		w.Header().Set("Content-Type", "application/json")
		if responses {
			_ = json.NewEncoder(w).Encode(response)
		} else {
			_ = json.NewEncoder(w).Encode(chat)
		}
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	frame := func(v any) {
		data, _ := json.Marshal(v)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
		w.(http.Flusher).Flush()
	}
	if responses {
		frame(map[string]any{"type": "response.output_text.delta", "delta": "begin", "item_id": "msg-1", "output_index": 0, "content_index": 0, "sequence_number": 1})
	} else {
		frame(map[string]any{"id": "owned-physical", "model": model, "choices": []any{map[string]any{"index": 0, "delta": map[string]any{"content": "begin"}, "finish_reason": nil}}})
	}
	time.Sleep(75 * time.Millisecond)
	if responses {
		frame(map[string]any{"type": "response.completed", "response": response, "sequence_number": 2})
	} else {
		frame(map[string]any{"id": "owned-physical", "model": model, "choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}}})
		frame(map[string]any{"id": "owned-physical", "model": model, "choices": []any{}, "usage": chat["usage"]})
	}
	_, _ = io.WriteString(w, "data: [DONE]\n\n")
}
func TestConcreteProducerHTTPOriginalGovernanceAndCancelledStreams(t *testing.T) {
	for _, gateway := range []bool{false, true} {
		for _, api := range []string{string(admission.Chat), string(admission.Responses)} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("gateway=%t/api=%s/stream=%t", gateway, api, stream), func(t *testing.T) {
					control, state := ownedPhysicalSocket(t)
					producer := exerciseConcreteCall(t, control, "home", []string{"owned-engine"}, gateway, api, stream, "")
					state.mu.Lock()
					defer state.mu.Unlock()
					if state.data != 1 || state.bad || producer.ActiveOperations() != 0 {
						t.Fatalf("dispatch mismatch: calls=%d bad=%t active=%d", state.data, state.bad, producer.ActiveOperations())
					}
				})
			}
		}
	}
}

func exerciseConcreteCall(t *testing.T, control *admission.Client, pool string, engines []string, gateway bool, api string, stream bool, caseName string) *admission.Producer {
	t.Helper()
	policy := &fundedPreparation{fixedPreparation: fixedPreparation{pool, engines}}
	producer, err := admission.NewProducer(control, policy, []schemas.ModelProvider{schemas.OpenAI}, 4, bifrost.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	f := fixtureWithInner(t, false, true, producer, pool, fixtureRouting{keylessEmpty: true})
	policy.governance = f.governance
	base, client := ownedHTTP(t, f)
	body := map[string]any{"model": "openai/" + model, "temperature": 0.25, "stream": stream}
	if api == string(admission.Chat) {
		body["messages"] = []any{map[string]any{"role": "user", "content": "owned"}}
		body["max_completion_tokens"] = 17
	} else {
		body["input"] = "owned"
		body["max_output_tokens"] = 17
	}
	if caseName != "" {
		if api == string(admission.Chat) {
			body["messages"] = []any{map[string]any{"role": "user", "content": caseName}}
		} else {
			body["input"] = caseName
		}
	}
	raw, _ := json.Marshal(body)
	path := "/openai" + api
	if gateway {
		path = admission.GatewayPrefix + api
	}
	request, _ := http.NewRequest(http.MethodPost, base+path, strings.NewReader(string(raw)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("x-bf-vk", vkValue)
	request.Header.Set("x-bf-passthrough-extra-params", "true")
	if gateway {
		request.Header.Set(admission.DelegationHeader, ownedHTTPToken(api, raw))
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 {
		data, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("status=%d body=%s", response.StatusCode, data)
	}
	if stream {
		one := make([]byte, 1)
		_, err = io.ReadFull(response.Body, one)
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
	expected := f.catalog.CalculateCost(&schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{Model: model, Usage: &schemas.BifrostLLMUsage{PromptTokens: 5, CompletionTokens: 2, TotalTokens: 7}, ExtraFields: schemas.BifrostResponseExtraFields{RequestType: schemas.ChatCompletionRequest, Provider: schemas.OpenAI, OriginalModelRequested: model, ResolvedModelUsed: model}}}, nil)
	if rate.TokenCurrentUsage != 44 || rate.RequestCurrentUsage != 5 || math.Abs(data.Budgets["original-budget"].CurrentUsage-(13.25+expected)) > 1e-9 {
		t.Fatalf("original accounting lost: tokens=%d requests=%d money=%v", rate.TokenCurrentUsage, rate.RequestCurrentUsage, data.Budgets["original-budget"].CurrentUsage)
	}
	if producer.ActiveOperations() != 0 || f.account.keys.Load() != 0 {
		t.Fatal("physical producer resources or key selection escaped")
	}
	return producer
}

func TestOwnedRustBifrostProducerProtocol(t *testing.T) {
	socket := os.Getenv("SW_BIFROST_OWNED_SOCKET")
	if socket == "" {
		t.Skip("requires explicitly owned Rust sender profile")
	}
	var engines []string
	if json.Unmarshal([]byte(os.Getenv("SW_BIFROST_OWNED_ENGINES")), &engines) != nil || len(engines) == 0 {
		t.Fatal("owned engine inventory missing")
	}
	for _, gateway := range []bool{false, true} {
		for _, api := range []string{string(admission.Chat), string(admission.Responses)} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("gateway=%t/api=%s/stream=%t", gateway, api, stream), func(t *testing.T) {
					control, err := admission.New(socket)
					if err != nil {
						t.Fatal(err)
					}
					defer control.Close()
					exerciseConcreteCall(t, control, "qwen", engines, gateway, api, stream, "bf-producer")
				})
			}
		}
	}

	t.Run("selected target budget denial", func(t *testing.T) {
		control, err := admission.New(socket)
		if err != nil {
			t.Fatal(err)
		}
		defer control.Close()
		producer, err := admission.NewProducer(control, deniedSelectedPreparation{fixedPreparation{"qwen", engines}}, []schemas.ModelProvider{schemas.OpenAI}, 4, bifrost.NewNoOpLogger())
		if err != nil {
			t.Fatal(err)
		}
		f := fixtureWithInner(t, false, true, producer, "qwen")
		ctx, cancel := ctxFor(vkValue)
		defer cancel()
		_, failure := f.client.ChatCompletionRequest(ctx, request(model))
		f.client.Shutdown()
		if failure == nil || failure.StatusCode == nil || *failure.StatusCode != 402 {
			t.Fatalf("real selected denial failed: %v", failure)
		}
		f.assertUntouched(t)
		if producer.ActiveOperations() != 0 {
			t.Fatal("denied real selected target retained resources")
		}
	})
	if t.Failed() {
		return
	}
	t.Log("owned_bifrost_rust_producer_passed=true; actual_canonical_http_cases=8; original_accounting_preserved=true; selected_target_denials=1")
}

func TestConcreteProducerRejectsBroaderPoolBeforeAdmission(t *testing.T) {
	control, state := ownedPhysicalSocket(t)
	producer, err := admission.NewProducer(control, fixedPreparation{"foreign", []string{"owned-engine"}}, []schemas.ModelProvider{schemas.OpenAI}, 4, bifrost.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	f := fixtureWithInner(t, false, true, producer, "home")
	ctx, cancel := ctxFor(vkValue)
	defer cancel()
	_, failure := f.client.ChatCompletionRequest(ctx, request(model))
	if failure == nil || failure.StatusCode == nil || *failure.StatusCode != 403 {
		t.Fatalf("broader target scope accepted: %v", failure)
	}
	f.client.Shutdown()
	f.assertUntouched(t)
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.requests) != 0 || state.data != 0 {
		t.Fatal("unauthorized preparation held capacity or dispatched")
	}
}
func TestConcreteProducerRejectsReplayedGatewayCallID(t *testing.T) {
	control, state := ownedPhysicalSocket(t)
	producer, err := admission.NewProducer(control, fixedPreparation{"home", []string{"owned-engine"}}, []schemas.ModelProvider{schemas.OpenAI}, 4, bifrost.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	f := fixtureWithInner(t, false, true, producer, "home")
	base, client := ownedHTTP(t, f)
	body := []byte(`{"model":"openai/gpt-4o","messages":[{"role":"user","content":"owned"}],"temperature":0.25,"max_completion_tokens":17}`)
	token := ownedHTTPToken(string(admission.Chat), body)
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodPost, base+admission.GatewayPrefix+string(admission.Chat), strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-bf-vk", vkValue)
		req.Header.Set(admission.DelegationHeader, token)
		response, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		expected := 200
		if i == 1 {
			expected = 503
		}
		if response.StatusCode != expected {
			t.Fatalf("replay status %d instead of%d", response.StatusCode, expected)
		}
	}
	f.client.Shutdown()
	data := f.store.GetGovernanceData(context.Background())
	if data.RateLimits["original-rate"].TokenCurrentUsage != 44 || data.RateLimits["original-rate"].RequestCurrentUsage != 5 {
		t.Fatal("replay spent original quota twice")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.data != 1 || len(state.requests) != 1 {
		t.Fatal("one signed call acquired another execution")
	}
}

type cancelledPreparation struct{ started chan struct{} }

func (p cancelledPreparation) AuthorizeSelected(_ *schemas.BifrostContext, _ *admission.Principal, _ admission.Lease, _ schemas.RequestType) (admission.Selection, *schemas.BifrostError) {
	return admission.Selection{}, &schemas.BifrostError{StatusCode: new(503), Error: &schemas.ErrorField{Message: "cancelled fixture must not dispatch"}}
}

func (p cancelledPreparation) Prepare(ctx *schemas.BifrostContext, _ admission.FrozenInput, _ *admission.Principal) (admission.Preparation, error) {
	close(p.started)
	<-ctx.Done()
	return admission.Preparation{}, ctx.Err()
}
func TestConcreteProducerCancellationBeforePreparationNeverQueues(t *testing.T) {
	control, state := ownedPhysicalSocket(t)
	started := make(chan struct{})
	producer, err := admission.NewProducer(control, cancelledPreparation{started}, []schemas.ModelProvider{schemas.OpenAI}, 4, bifrost.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	f := fixtureWithInner(t, false, true, producer, "home")
	ctx, cancel := ctxFor(vkValue)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _, _ = f.client.ChatCompletionRequest(ctx, request(model)) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("preparation did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("preparation ignored original cancellation")
	}
	f.client.Shutdown()
	f.assertUntouched(t)
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.requests) != 0 || state.data != 0 || producer.ActiveOperations() != 0 {
		t.Fatal("cancelled preparation retained work")
	}
}

// Before this guard was wired, a prepared physical target could start inference
// even though its selected-target financial policy explicitly refused it.
type deniedSelectedPreparation struct{ fixedPreparation }

func (p deniedSelectedPreparation) AuthorizeSelected(_ *schemas.BifrostContext, _ *admission.Principal, _ admission.Lease, _ schemas.RequestType) (admission.Selection, *schemas.BifrostError) {
	return admission.Selection{}, &schemas.BifrostError{StatusCode: new(402), Error: &schemas.ErrorField{Message: "selected target budget exhausted"}}
}
func TestConcreteProducerSelectedBudgetDenialNeverDispatchesOrSpends(t *testing.T) {
	control, state := ownedPhysicalSocket(t)
	producer, err := admission.NewProducer(control, deniedSelectedPreparation{fixedPreparation{"home", []string{"owned-engine"}}}, []schemas.ModelProvider{schemas.OpenAI}, 4, bifrost.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	f := fixtureWithInner(t, false, true, producer, "home")
	ctx, cancel := ctxFor(vkValue)
	defer cancel()
	_, failure := f.client.ChatCompletionRequest(ctx, request(model))
	f.client.Shutdown()
	if failure == nil || failure.StatusCode == nil || *failure.StatusCode != 402 {
		t.Fatalf("selected target policy did not block inference: %v", failure)
	}
	f.assertUntouched(t)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.data != 0 || producer.ActiveOperations() != 0 {
		t.Fatal("denied selected target dispatched or retained producer resources")
	}
}

type selectedFaultPreparation struct {
	fixedPreparation
	kind   string
	cancel context.CancelFunc
}

func (p selectedFaultPreparation) AuthorizeSelected(_ *schemas.BifrostContext, _ *admission.Principal, lease admission.Lease, typ schemas.RequestType) (admission.Selection, *schemas.BifrostError) {
	target := admission.ModelTarget{Pool: lease.Pool, Provider: schemas.OpenAI, Model: model}
	switch p.kind {
	case "pool":
		target.Pool = "foreign"
	case "scope":
		target.Model = "not-granted"
	case "cancel":
		p.cancel()
	}
	funding := admission.SelectedFunding(fixtureFunding{target: schemas.Route{Provider: schemas.OpenAI, Model: model}, typ: typ})
	if p.kind == "funding" {
		funding = fixtureFunding{target: schemas.Route{Provider: schemas.OpenAI, Model: "different-funded-model"}, typ: typ}
	}
	if p.kind == "type" {
		funding = fixtureFunding{target: schemas.Route{Provider: schemas.OpenAI, Model: model}, typ: schemas.ResponsesRequest}
	}
	if p.kind == "missing-funding" {
		funding = nil
	}
	return admission.Selection{Target: target, Funding: funding}, nil
}
func TestConcreteProducerSelectedTargetScopeOrCancellationNeverDispatches(t *testing.T) {
	for _, kind := range []string{"pool", "scope", "cancel", "funding", "missing-funding", "type"} {
		t.Run(kind, func(t *testing.T) {
			control, state := ownedPhysicalSocket(t)
			ctx, cancel := ctxFor(vkValue)
			defer cancel()
			producer, err := admission.NewProducer(control, selectedFaultPreparation{fixedPreparation{"home", []string{"owned-engine"}}, kind, cancel}, []schemas.ModelProvider{schemas.OpenAI}, 4, bifrost.NewNoOpLogger())
			if err != nil {
				t.Fatal(err)
			}
			f := fixtureWithInner(t, false, true, producer, "home")
			_, failure := f.client.ChatCompletionRequest(ctx, request(model))
			f.client.Shutdown()
			expected := 403
			if kind == "cancel" {
				expected = 408
			}
			if kind == "missing-funding" {
				expected = 503
			}
			if failure == nil || failure.StatusCode == nil || *failure.StatusCode != expected {
				t.Fatalf("selected %s status: %v", kind, failure)
			}
			f.assertUntouched(t)
			state.mu.Lock()
			defer state.mu.Unlock()
			if state.data != 0 || producer.ActiveOperations() != 0 {
				t.Fatal("unqualified selected target dispatched or retained resources")
			}
		})
	}
}
