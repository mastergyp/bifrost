package admission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/maximhq/bifrost/core/providers/openai"
	wire "github.com/maximhq/bifrost/core/providers/utils"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

// Preparation is computed by the deployment's immutable registry/health and
// target-contract policy before reserving a physical slot. It grants no access:
// Producer intersects Pools with the original admitted principal again.
type Preparation struct {
	Pools          []string
	HealthyEngines []string
	Cost           uint64
	NativeWeight   uint32
}

// FrozenInput exposes a reader over the exact bytes admission and dispatch use.
// Policies cannot rewrite the body they are approving or resurrect source defaults.
type FrozenInput struct {
	api  API
	body []byte
}

func (FrozenInput) String() string      { return "managed inference input (redacted)" }
func (i FrozenInput) GoString() string  { return i.String() }
func (i FrozenInput) API() API          { return i.api }
func (i FrozenInput) Size() int         { return len(i.body) }
func (i FrozenInput) Reader() io.Reader { return bytes.NewReader(i.body) }

type PreparationPolicy interface {
	Prepare(*schemas.BifrostContext, FrozenInput, *Principal) (Preparation, error)
}

// SelectedTargetPolicy rechecks the actually selected private backend after
// queueing and before I/O. It must preserve/bind the original financial payers
// and validate fresh target authority; it must not execute inference or wait
// for a nested physical reservation while holding this prepared ticket.
type SelectedTargetPolicy interface {
	AuthorizeSelected(*schemas.BifrostContext, *Principal, Lease, schemas.RequestType) (Selection, *schemas.BifrostError)
}

// SelectedFunding is an opaque approval issued by the original governance
// owner. Producer verifies its exact target and binds it before any data I/O;
// a preparation policy cannot accidentally approve one target and bill another.
type SelectedFunding interface {
	Target() schemas.Route
	RequestType() schemas.RequestType
	Bind(*schemas.BifrostContext) *schemas.BifrostError
}

type Selection struct {
	Target  ModelTarget
	Funding SelectedFunding
}

// Producer uses the existing OpenAI converters, serializers and SSE decoders.
// Only the fixed local Unix sender is contacted. The CLI does not install it.
// A production profile must qualify its registry/target policy and all entrances.
type Producer struct {
	client    *Client
	wire      *fasthttp.Client
	policy    PreparationPolicy
	selected  SelectedTargetPolicy
	logger    schemas.Logger
	providers map[schemas.ModelProvider]bool
	permits   chan struct{}
	mu        sync.Mutex
	closed    bool
}

func NewProducer(client *Client, policy PreparationPolicy, providers []schemas.ModelProvider, maxOperations int, logger schemas.Logger) (*Producer, error) {
	selected, qualified := policy.(SelectedTargetPolicy)
	if client == nil || client.socket == "" || policy == nil || !qualified || logger == nil || len(providers) == 0 || len(providers) > 32 || maxOperations < 1 || maxOperations > 4096 {
		return nil, ErrRejected
	}
	p := &Producer{client: client, policy: policy, selected: selected, logger: logger, providers: map[schemas.ModelProvider]bool{}, permits: make(chan struct{}, maxOperations)}
	for _, provider := range providers {
		if !boundedIdentity(string(provider)) || p.providers[provider] {
			return nil, ErrRejected
		}
		p.providers[provider] = true
	}
	p.wire = &fasthttp.Client{Dial: func(_ string) (net.Conn, error) { return net.DialTimeout("unix", client.socket, time.Second) }, MaxConnsPerHost: maxOperations, MaxIdleConnDuration: 30 * time.Second, MaxIdemponentCallAttempts: 1, MaxResponseBodySize: maxResponseBytes, ReadBufferSize: 65536}
	return p, nil
}
func (*Producer) String() string     { return "managed physical producer (redacted)" }
func (p *Producer) GoString() string { return p.String() }
func (p *Producer) Owns(provider schemas.ModelProvider) bool {
	return p != nil && p.providers[provider]
}
func (p *Producer) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.wire.CloseIdleConnections()
	p.client.Close()
}
func (p *Producer) ActiveOperations() int { return len(p.permits) }
func producerError(status int, message string) *schemas.LLMPluginShortCircuit {
	return &schemas.LLMPluginShortCircuit{Error: &schemas.BifrostError{StatusCode: &status, IsBifrostError: true, AllowFallbacks: new(false), Error: &schemas.ErrorField{Message: message}}}
}
func (p *Producer) enter() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	select {
	case p.permits <- struct{}{}:
		return true
	default:
		return false
	}
}
func (p *Producer) leave() {
	<-p.permits
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		p.wire.CloseIdleConnections()
		p.client.Close()
	}
}

// freezeWire reuses canonical conversion, then restores only the original
// explicit controls. Source-model defaults must not become target authority.
// Bodies remain on the owned request, never retained in shared context values.
func freezeWire(ctx *schemas.BifrostContext, r *schemas.BifrostRequest, principal *Principal) (API, []byte, error) {
	var api API
	var converted wire.RequestBodyWithExtraParams
	stream := false
	switch r.RequestType {
	case schemas.ChatCompletionRequest, schemas.ChatCompletionStreamRequest:
		if r.ChatRequest == nil {
			return "", nil, ErrRejected
		}
		api = Chat
		stream = r.RequestType == schemas.ChatCompletionStreamRequest
		chat := openai.ToOpenAIChatRequest(ctx, r.ChatRequest)
		if chat == nil {
			return "", nil, ErrRejected
		}
		if err := openai.ResolveChatFileURLs(ctx, r.ChatRequest.Provider, chat); err != nil {
			return "", nil, ErrRejected
		}
		converted = chat
	case schemas.ResponsesRequest, schemas.ResponsesStreamRequest:
		if r.ResponsesRequest == nil {
			return "", nil, ErrRejected
		}
		api = Responses
		stream = r.RequestType == schemas.ResponsesStreamRequest
		converted = openai.ToOpenAIResponsesRequest(ctx, r.ResponsesRequest)
	default:
		return "", nil, ErrRejected
	}
	if converted == nil {
		return "", nil, ErrRejected
	}
	raw, err := wire.MarshalProviderRequest(converted)
	if err != nil {
		return "", nil, ErrRejected
	}
	if extra, _ := ctx.Value(schemas.BifrostContextKeyPassthroughExtraParams).(bool); extra {
		raw, err = wire.MergeExtraParamsIntoJSON(raw, converted.GetExtraParams())
		if err != nil {
			return "", nil, ErrRejected
		}
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return "", nil, ErrRejected
	}
	controls := principal.controls
	if controls == nil {
		// Direct SDK composition has no HTTP snapshot. Freeze its typed controls
		// here; every production entrance must capture before source defaults/hooks.
		var params any
		if r.ChatRequest != nil {
			params = r.ChatRequest.Params
		} else {
			params = r.ResponsesRequest.Params
		}
		typed, e := json.Marshal(params)
		if e != nil {
			return "", nil, ErrRejected
		}
		if string(typed) == "null" {
			controls = map[string]json.RawMessage{}
		} else {
			controls, e = extractControls(typed)
			if e != nil {
				return "", nil, e
			}
		}
		for k, v := range converted.GetExtraParams() {
			if controlFields[k] {
				b, e := json.Marshal(v)
				if e != nil {
					return "", nil, ErrRejected
				}
				controls[k] = b
			}
		}
	}
	for name := range controlFields {
		delete(fields, name)
	}
	for name, value := range controls {
		fields[name] = bytes.Clone(value)
	}
	fields["stream"], _ = json.Marshal(stream)
	if stream && api == Chat {
		fields["stream_options"] = json.RawMessage(`{"include_usage":true}`)
	}
	if b := fields["background"]; len(b) > 0 && string(b) != "false" && string(b) != "null" {
		return "", nil, ErrRejected
	}
	if b := fields["fallbacks"]; len(b) > 0 && string(b) != "[]" && string(b) != "null" {
		return "", nil, ErrRejected
	}
	if _, ok := fields["provider"]; ok {
		return "", nil, ErrRejected
	}
	raw, err = json.Marshal(fields)
	if err != nil || len(raw) == 0 || len(raw) > maxInferenceBytes {
		return "", nil, ErrRejected
	}
	if principal.delegation != nil && string(api) != principal.delegation.claims.API {
		return "", nil, ErrRejected
	}
	return api, raw, nil
}
func extractControls(raw []byte) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return nil, ErrRejected
	}
	controls := map[string]json.RawMessage{}
	for k, v := range fields {
		if controlFields[k] {
			controls[k] = bytes.Clone(v)
		}
	}
	b, e := json.Marshal(controls)
	if e != nil || len(b) > 3072 {
		return nil, ErrRejected
	}
	return controls, nil
}
func (p *Producer) Handle(ctx *schemas.BifrostContext, r *schemas.BifrostRequest) (result *schemas.LLMPluginShortCircuit) {
	principal := AdmittedPrincipal(ctx)
	if ctx == nil || r == nil || principal == nil || principal.source.Alias == "" {
		return producerError(403, "managed original principal required")
	}
	requestType := r.RequestType // the core request is pooled; protocol callbacks own only this scalar
	if !p.enter() {
		return producerError(503, "managed execution admission is closed or full")
	}
	retained := false
	defer func() {
		if !retained {
			p.leave()
		}
	}()
	if principal.controlsInvalid {
		return producerError(400, "original controls exceed managed contract")
	}
	planning := schemas.NewBifrostContext(ctx, principal.deadline)
	defer planning.Cancel()
	// BifrostContext propagates parent cancellation in a watcher goroutine.
	// Check the original caller and clock directly as well, so that scheduling
	// delay cannot turn a cancellation during final validation into dispatch.
	planningEnded := func() bool {
		return ctx.Err() != nil || planning.Err() != nil || !time.Now().Before(principal.deadline)
	}
	api, body, err := freezeWire(planning, r, principal)
	if planningEnded() {
		return producerError(408, "original managed preparation deadline exceeded")
	}
	if err != nil {
		return producerError(400, "request is not supported by the managed model contract")
	}
	plan, err := p.policy.Prepare(planning, FrozenInput{api: api, body: body}, principal)
	if planningEnded() {
		return producerError(408, "original managed preparation deadline exceeded")
	}
	if err != nil {
		if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrNotEnforced) {
			return producerError(503, "managed target preparation is unavailable")
		}
		return producerError(400, "request does not match an approved target contract")
	}
	pools := []string{}
	seen := map[string]bool{}
	for _, pool := range plan.Pools {
		if !contains(principal.authorization.Pools, pool) || seen[pool] {
			return producerError(403, "target exceeds original model scope")
		}
		seen[pool] = true
		pools = append(pools, pool)
	}
	digest, _ := RequestDigest(api, body)
	operation := Request{ID: uuid.NewString(), Owner: uuid.NewString(), SubjectHash: principal.authorization.SubjectHash, SessionHash: principal.SessionHash(), BodyHash: digest, Alias: principal.source.Alias, EligiblePools: pools, HealthyEngines: append([]string{}, plan.HealthyEngines...)}
	weight := plan.NativeWeight
	if proof := principal.delegation; proof != nil {
		operation.ID = proof.CallID()
		weight = proof.claims.Weight
	}
	if len(pools) == 0 || !validRequest(operation) || plan.Cost == 0 || weight < 1 || weight > 1000 {
		return producerError(400, "invalid managed preparation")
	}
	owned := schemas.NewBifrostContext(context.Background(), time.Now().Add(20*time.Minute))
	owned.SetGrant(ctx.Grant())
	owned.SetValue(schemas.BifrostContextKeyUseRawRequestBody, true)
	wire.EnableManagedWireAccounting(owned)
	transferred := false
	defer func() {
		if !transferred {
			owned.Cancel()
		}
	}()
	var ticket *Ticket
	var actualRoute *schemas.ManagedExecutionRoute
	signer := func(actual []byte) (map[string]string, *schemas.BifrostError) {
		if !bytes.Equal(actual, body) || ticket != nil {
			return nil, producerError(503, "managed body changed or dispatch replayed").Error
		}
		ticket, err = p.client.AcquireUntil(planning, operation, plan.Cost, weight, principal.deadline)
		if err != nil {
			status := 503
			if errors.Is(err, ErrExpired) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				status = 408
			}
			return nil, producerError(status, "managed admission did not dispatch").Error
		}
		if planningEnded() {
			cleanup, done := context.WithTimeout(context.Background(), time.Second)
			defer done()
			_, _ = ticket.Cancel(cleanup)
			return nil, producerError(408, "managed caller left before dispatch").Error
		}
		selection, refusal := p.selected.AuthorizeSelected(ctx, principal, ticket.Lease, requestType)
		if refusal != nil {
			copy := *refusal
			copy.AllowFallbacks = new(false)
			return nil, &copy
		}
		if planningEnded() {
			return nil, producerError(408, "managed selected-target validation deadline exceeded").Error
		}
		target := selection.Target
		if !validTarget(target) || target.Pool != ticket.Lease.Pool || !p.Owns(target.Provider) ||
			ctx.Grant() == nil || ctx.Grant().Access() == nil || !ctx.Grant().Access().IsModelAllowed(string(target.Provider), target.Model) {
			return nil, producerError(403, "selected target no longer matches original authority").Error
		}
		if keys, restricted := ctx.Grant().Access().KeysForModel(string(target.Provider), target.Model); restricted && len(keys) != 0 {
			return nil, producerError(403, "keyless managed target exceeds provider-key scope").Error
		}
		if selection.Funding == nil {
			return nil, producerError(503, "selected managed target funding is missing").Error
		}
		approved := selection.Funding.Target()
		if approved.Provider != target.Provider || approved.Model != target.Model || approved.KeyID != "" || selection.Funding.RequestType() != requestType {
			return nil, producerError(403, "selected target does not match its funding approval").Error
		}
		if denied := selection.Funding.Bind(ctx); denied != nil {
			copy := *denied
			copy.AllowFallbacks = new(false)
			return nil, &copy
		}
		if planningEnded() {
			return nil, producerError(408, "managed caller left before funded dispatch").Error
		}
		// Read only the approved selected target's server-owned stream policy.
		// Source/request context cannot override the selected model after spill.
		idle := wire.DefaultStreamIdleTimeout
		if configured, ok := selection.Funding.(interface{ StreamIdleTimeout() time.Duration }); ok {
			if value := configured.StreamIdleTimeout(); value > 0 {
				idle = value
			}
		}
		owned.SetValue(schemas.BifrostContextKeyStreamIdleTimeout, idle)
		actualRoute = &schemas.ManagedExecutionRoute{Requested: principal.OriginalRoute(), Resolved: schemas.Route{Provider: target.Provider, Model: target.Model}, Foreign: ticket.Lease.Foreign}
		ctx.SetRoutingInfoSnapshot(actualRoute.RoutingInfo())
		headers := map[string]string{"X-SW-Admission-Id": operation.ID, "X-SW-Admission-Owner": operation.Owner, "X-SW-Admission-Alias": operation.Alias}
		p.client.setAuthority(func(k, v string) { headers[k] = v })
		return headers, nil
	}
	cleanupPrepared := func(confirm bool) bool {
		if ticket != nil {
			cleanup, done := context.WithTimeout(context.Background(), time.Second)
			defer done()
			if confirm {
				confirmed, err := ticket.ConfirmNeverDispatched(cleanup)
				return err == nil && confirmed
			}
			_, _ = ticket.Cancel(cleanup)
		}
		return false
	}
	defer func() {
		if !transferred {
			confirmed := cleanupPrepared(result != nil && result.Error != nil)
			if confirmed && principal.delegation != nil {
				if envelope, ok := ctx.Value(gatewayProofKey).(*gatewayEnvelope); ok && envelope != nil && envelope.proof == principal.delegation {
					envelope.noDispatch.Store(true)
				}
			}
		}
	}()
	url := "http://localhost/inference" + string(api)
	provider, _, _ := r.GetRequestFields()
	pass := func(_ *schemas.BifrostContext, response *schemas.BifrostResponse, e *schemas.BifrostError) (*schemas.BifrostResponse, *schemas.BifrostError) {
		return response, e
	}
	var stream chan *schemas.BifrostStreamChunk
	var failure *schemas.BifrostError
	switch requestType {
	case schemas.ChatCompletionRequest:
		request := *r.ChatRequest
		request.RawRequestBody = body
		response, e := openai.HandleOpenAIChatCompletionRequest(owned, p.wire, url, &request, nil, nil, false, false, provider, nil, nil, signer, p.logger)
		if e != nil {
			return &schemas.LLMPluginShortCircuit{Error: e, ManagedRoute: actualRoute}
		}
		return &schemas.LLMPluginShortCircuit{Response: &schemas.BifrostResponse{ChatResponse: response}, ManagedRoute: actualRoute}
	case schemas.ResponsesRequest:
		request := *r.ResponsesRequest
		request.RawRequestBody = body
		response, e := openai.HandleOpenAIResponsesRequest(owned, p.wire, url, &request, nil, nil, false, false, provider, nil, nil, signer, p.logger)
		if e != nil {
			return &schemas.LLMPluginShortCircuit{Error: e, ManagedRoute: actualRoute}
		}
		return &schemas.LLMPluginShortCircuit{Response: &schemas.BifrostResponse{ResponsesResponse: response}, ManagedRoute: actualRoute}
	case schemas.ChatCompletionStreamRequest:
		request := *r.ChatRequest
		request.RawRequestBody = body
		stream, failure = openai.HandleOpenAIChatCompletionStreaming(owned, p.wire, url, &request, nil, nil, 0, false, false, provider, pass, nil, nil, nil, nil, nil, signer, p.logger, nil)
	case schemas.ResponsesStreamRequest:
		request := *r.ResponsesRequest
		request.RawRequestBody = body
		stream, failure = openai.HandleOpenAIResponsesStreaming(owned, p.wire, url, &request, nil, nil, 0, false, false, provider, pass, nil, nil, nil, nil, signer, p.logger, nil)
	}
	if failure != nil {
		return &schemas.LLMPluginShortCircuit{Error: failure, ManagedRoute: actualRoute}
	}
	output := make(chan *schemas.BifrostStreamChunk, schemas.DefaultStreamBufferSize)
	retained = true
	transferred = true
	go func() {
		defer func() { cleanupPrepared(false); owned.Cancel(); p.leave(); close(output) }()
		for chunk := range stream {
			select {
			case output <- chunk:
			case <-owned.Done():
				return
			}
		}
	}()
	return &schemas.LLMPluginShortCircuit{Stream: output, ManagedRoute: actualRoute}
}
