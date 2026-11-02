package admission

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

const GatewayPrefix = "/sw/gateway"

type provenanceKey uint8

const (
	gatewayProofKey provenanceKey = iota
	nativeSessionKey
	principalKey
	nativeBudgetKey
)

// RuntimeModel is immutable deployment input. Provider/model is explicit so
// catalog inference cannot silently change the signed source contract.
type RuntimeModel struct {
	Alias string
	ModelTarget
}

// NativeModelAlias explicitly binds a logical native model to its registered
// source contract. A pre-request routing hook may translate this alias, but
// cannot replace the original authorization or quality contract with its target.
type NativeModelAlias struct {
	Provider    schemas.ModelProvider
	Model       string
	SourceAlias string
}

type gatewayEnvelope struct {
	proof      *VerifiedDelegation
	source     RuntimeModel
	noDispatch atomic.Bool
}

// Principal is a small admitted handle, not request/stream storage. Only this
// boundary constructs it, after existing governance resolved the original Grant.
type Principal struct {
	authorization   NativeAuthorization
	session         *string
	delegation      *VerifiedDelegation
	source          RuntimeModel
	deadline        time.Time
	controls        map[string]json.RawMessage
	controlsInvalid bool
	original        schemas.Route
	routed          ModelTarget
}

type nativeOrigin struct {
	source    *RuntimeModel
	requested schemas.Route
	required  bool
}
type nativeBudget struct {
	deadline time.Time
	controls map[string]json.RawMessage
	invalid  bool
	http     bool
	origin   nativeOrigin
}

func (p *Principal) Authorization() NativeAuthorization {
	if p == nil {
		return NativeAuthorization{}
	}
	a := p.authorization
	a.Pools = append([]string(nil), a.Pools...)
	return a
}
func (p *Principal) SessionHash() *string {
	if p == nil || p.session == nil {
		return nil
	}
	s := *p.session
	return &s
}
func (p *Principal) Source() RuntimeModel {
	if p == nil {
		return RuntimeModel{}
	}
	return p.source
}
func (p *Principal) QueueDeadline() time.Time {
	if p == nil {
		return time.Time{}
	}
	return p.deadline
}
func (p *Principal) OriginalRoute() schemas.Route {
	if p == nil {
		return schemas.Route{}
	}
	return p.original
}
func (p *Principal) RoutedTarget() ModelTarget {
	if p == nil {
		return ModelTarget{}
	}
	return p.routed
}
func (p *Principal) Delegation() *VerifiedDelegation {
	if p == nil {
		return nil
	}
	return p.delegation
}
func AdmittedPrincipal(ctx *schemas.BifrostContext) *Principal {
	if ctx == nil {
		return nil
	}
	p, _ := ctx.Value(principalKey).(*Principal)
	return p
}

// Runtime composes provenance with the existing core managed boundary. A shared
// legacy forwarding VK keeps native home-model access; it cannot spill as an
// employee without a verified Gateway channel. No Grant or Limits is replaced.
// The inner producer must enforce physical capacity, target contracts and usage;
// this wrapper alone is not release qualification for those responsibilities.
type Runtime struct {
	inner     schemas.ManagedInference
	native    *NativeAuthorizer
	verifier  *DelegationVerifier
	models    []RuntimeModel
	sharedIDs map[string]bool
	aliases   []NativeModelAlias
}

func NewRuntime(inner schemas.ManagedInference, native *NativeAuthorizer, verifier *DelegationVerifier, models []RuntimeModel, sharedLegacyVKIDs []string, nativeAliases ...NativeModelAlias) (*Runtime, error) {
	if inner == nil || native == nil || len(native.secret) < 32 || verifier == nil || len(verifier.keys) == 0 || len(models) == 0 || len(models) > 32 || len(sharedLegacyVKIDs) > 128 || len(nativeAliases) > 128 {
		return nil, ErrRejected
	}
	r := &Runtime{inner: inner, native: native, verifier: verifier, models: append([]RuntimeModel(nil), models...), sharedIDs: map[string]bool{}}
	for _, key := range verifier.keys {
		if bytes.Equal(native.secret, key.secret) {
			return nil, ErrRejected
		}
	}
	aliases, wires, pools := map[string]bool{}, map[string]string{}, map[string]ModelTarget{}
	for _, m := range models {
		wire := string(m.Provider) + "/" + m.Model
		if !delegationID(m.Alias) || !validTarget(m.ModelTarget) || !inner.Owns(m.Provider) || aliases[m.Alias] {
			return nil, ErrRejected
		}
		if pool, ok := wires[wire]; ok && pool != m.Pool {
			return nil, ErrRejected
		}
		if target, ok := pools[m.Pool]; ok && target != m.ModelTarget {
			return nil, ErrRejected
		}
		aliases[m.Alias], wires[wire], pools[m.Pool] = true, m.Pool, m.ModelTarget
	}
	for _, id := range sharedLegacyVKIDs {
		if !boundedIdentity(id) || r.sharedIDs[id] {
			return nil, ErrRejected
		}
		if _, denied := native.delegated[id]; denied {
			return nil, ErrRejected // home-only legacy IDs cannot also be globally denied
		}
		r.sharedIDs[id] = true
	}
	bindings := map[string]bool{}
	for _, alias := range nativeAliases {
		wire := string(alias.Provider) + "/" + alias.Model
		if !boundedIdentity(string(alias.Provider)) || !boundedIdentity(alias.Model) || !aliases[alias.SourceAlias] || bindings[wire] {
			return nil, ErrRejected
		}
		// A logical alias cannot replace another registered model's authority.
		for _, model := range models {
			if wire == string(model.Provider)+"/"+model.Model && alias.SourceAlias != model.Alias {
				return nil, ErrRejected
			}
		}
		bindings[wire] = true
	}
	r.aliases = append([]NativeModelAlias(nil), nativeAliases...)
	return r, nil
}

func (r *Runtime) nativeOrigin(provider schemas.ModelProvider, model string) nativeOrigin {
	if !boundedIdentity(model) || provider != "" && !boundedIdentity(string(provider)) {
		return nativeOrigin{required: r.Owns(provider)}
	}
	origin := nativeOrigin{requested: schemas.Route{Provider: schemas.ModelProvider(strings.Clone(string(provider))), Model: strings.Clone(model)}, required: r.Owns(provider)}
	for _, alias := range r.aliases {
		if alias.Model == model && (provider == "" || alias.Provider == provider) {
			for i := range r.models {
				if r.models[i].Alias == alias.SourceAlias {
					if origin.source != nil && origin.source.Alias != alias.SourceAlias {
						origin.source = nil
						origin.required = true
						return origin
					}
					origin.source = &r.models[i]
					origin.requested.Provider = alias.Provider
				}
			}
		}
	}
	if origin.source == nil {
		for i := range r.models {
			m := &r.models[i]
			if m.Model == model && (provider == "" || m.Provider == provider) {
				if origin.source != nil && origin.source.ModelTarget != m.ModelTarget {
					origin.source = nil
					origin.required = true
					return origin
				}
				if origin.source == nil {
					origin.source = m
					origin.requested.Provider = m.Provider
				}
			}
		}
	}
	origin.required = origin.required || origin.source != nil
	return origin
}

func (r *Runtime) nativeHTTPOrigin(model string) nativeOrigin {
	// Prefer a registered explicit provider prefix. A model name may itself
	// contain '/', so an arbitrary prefix is never guessed to be a provider.
	if provider, name, ok := strings.Cut(model, "/"); ok {
		if origin := r.nativeOrigin(schemas.ModelProvider(provider), name); origin.required {
			return origin
		}
	}
	return r.nativeOrigin("", model)
}

// CaptureRequest runs before core pre-request hooks and session routing. HTTP
// metadata was frozen still earlier, before HTTP transport hooks. Direct SDK
// calls freeze only bounded controls, model and deadline here, never payloads.
func (r *Runtime) CaptureRequest(ctx *schemas.BifrostContext, req *schemas.BifrostRequest) {
	if r == nil || ctx == nil || req == nil || r.requiresDelegation(ctx) {
		return
	}
	if budget, ok := ctx.Value(nativeBudgetKey).(*nativeBudget); ok && budget != nil && budget.http {
		return
	}
	provider, model, _ := req.GetRequestFields()
	budget := &nativeBudget{deadline: time.Now().Add(60 * time.Second), origin: r.nativeOrigin(provider, model)}
	budget.controls, budget.invalid = sdkControls(req)
	ctx.SetValue(nativeBudgetKey, budget)
}

func sdkControls(req *schemas.BifrostRequest) (map[string]json.RawMessage, bool) {
	var params any
	var extra map[string]interface{}
	if req.ChatRequest != nil && req.ChatRequest.Params != nil {
		params, extra = req.ChatRequest.Params, req.ChatRequest.Params.ExtraParams
	} else if req.ResponsesRequest != nil && req.ResponsesRequest.Params != nil {
		params, extra = req.ResponsesRequest.Params, req.ResponsesRequest.Params.ExtraParams
	}
	controls := map[string]json.RawMessage{}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return nil, true
		}
		controls, err = extractControls(raw)
		if err != nil {
			return nil, true
		}
	}
	for k, v := range extra {
		if controlFields[k] {
			raw, err := json.Marshal(v)
			if err != nil {
				return nil, true
			}
			controls[k] = raw
		}
	}
	raw, err := json.Marshal(controls)
	return controls, err != nil || len(raw) > 3072
}

func (*Runtime) String() string                { return "managed admission runtime (redacted)" }
func (r *Runtime) GoString() string            { return r.String() }
func (*Principal) String() string              { return "admitted principal (redacted)" }
func (p *Principal) GoString() string          { return p.String() }
func (*DelegationVerifier) String() string     { return "delegation verifier (redacted)" }
func (v *DelegationVerifier) GoString() string { return v.String() }
func (*NativeAuthorizer) String() string       { return "native authorizer (redacted)" }
func (a *NativeAuthorizer) GoString() string   { return a.String() }
func (r *Runtime) Owns(provider schemas.ModelProvider) bool {
	return r != nil && r.inner != nil && r.inner.Owns(provider)
}
func (r *Runtime) RequiresManaged(ctx *schemas.BifrostContext) bool {
	if ctx == nil {
		return false
	}
	if r.requiresDelegation(ctx) {
		return true
	}
	budget, ok := ctx.Value(nativeBudgetKey).(*nativeBudget)
	return ok && budget != nil && budget.origin.required
}
func (r *Runtime) requiresDelegation(ctx *schemas.BifrostContext) bool {
	if ctx == nil {
		return false
	}
	envelope, ok := ctx.Value(gatewayProofKey).(*gatewayEnvelope)
	return ok && envelope != nil && envelope.proof != nil
}

// No current cache receipt binds actual model, original scope and registry
// generation. Reject unqualified shortcuts in the opt-in managed profile;
// never claim that the source logical label proves its cached target is allowed.
// Existing unconfigured/native caching is unchanged. Cache compatibility must
// be qualified before this profile is production-enabled.
func (r *Runtime) ValidateShortcut(*schemas.BifrostContext, *schemas.BifrostRequest, *schemas.LLMPluginShortCircuit) *schemas.BifrostError {
	return &schemas.BifrostError{StatusCode: new(503), IsBifrostError: true, AllowFallbacks: new(false), Error: &schemas.ErrorField{Message: "managed cached result is not qualified"}}
}
func (r *Runtime) Close() {
	if r != nil && r.inner != nil {
		r.inner.Close()
	}
}
func provenanceError(status int) *schemas.LLMPluginShortCircuit {
	message := "managed original-caller authorization failed"
	if status == 408 {
		message = "managed original queue deadline exceeded"
	}
	return &schemas.LLMPluginShortCircuit{Error: &schemas.BifrostError{StatusCode: &status, IsBifrostError: true, AllowFallbacks: new(false), Error: &schemas.ErrorField{Message: message}}}
}
func (r *Runtime) Handle(ctx *schemas.BifrostContext, req *schemas.BifrostRequest) *schemas.LLMPluginShortCircuit {
	return r.handleAt(ctx, req, time.Now())
}
func (r *Runtime) handleAt(ctx *schemas.BifrostContext, req *schemas.BifrostRequest, now time.Time) *schemas.LLMPluginShortCircuit {
	if r == nil || ctx == nil || req == nil {
		return provenanceError(503)
	}
	provider, model, _ := req.GetRequestFields()
	envelope, delegated := ctx.Value(gatewayProofKey).(*gatewayEnvelope)
	if delegated && (envelope == nil || envelope.proof == nil) {
		return provenanceError(403)
	}
	var source *RuntimeModel
	for i := range r.models {
		if r.models[i].Provider == provider && r.models[i].Model == model && (!delegated || r.models[i].Alias == envelope.source.Alias) {
			source = &r.models[i]
			break
		}
	}
	if source == nil {
		return provenanceError(403)
	}
	routed := source.ModelTarget
	original := schemas.Route{Provider: source.Provider, Model: source.Model}
	if !delegated {
		if budget, ok := ctx.Value(nativeBudgetKey).(*nativeBudget); ok && budget != nil {
			if budget.origin.source == nil {
				return provenanceError(403)
			}
			source = budget.origin.source
			original = budget.origin.requested
		}
	}
	var auth NativeAuthorization
	var session *string
	var proof *VerifiedDelegation
	var err error
	if delegated {
		if envelope == nil || envelope.proof == nil || envelope.source != *source {
			return provenanceError(403)
		}
		targets := make([]DelegationTarget, 0, len(r.models))
		seen := map[string]bool{}
		for _, m := range r.models {
			if seen[m.Pool] || !contains(envelope.proof.claims.AllowedAliases, m.Alias) || m.Pool == source.Pool && m.Alias != source.Alias {
				continue
			}
			seen[m.Pool] = true
			targets = append(targets, DelegationTarget{Alias: m.Alias, ModelTarget: m.ModelTarget})
		}
		proof = envelope.proof
		auth, err = proof.Authorize(ctx, DelegationTarget{Alias: source.Alias, ModelTarget: source.ModelTarget}, targets, now)
		session = proof.SessionHash()
	} else {
		grant := ctx.Grant()
		if grant == nil || grant.Access() == nil || !grant.Access().IsModelAllowed(string(original.Provider), original.Model) {
			return provenanceError(403)
		}
		targets := make([]ModelTarget, 0, len(r.models))
		seen := map[string]bool{}
		for _, m := range r.models {
			if !seen[m.Pool] {
				seen[m.Pool] = true
				targets = append(targets, m.ModelTarget)
			}
		}
		auth, err = r.native.Authorize(ctx, source.ModelTarget, targets)
		if err == nil {
			if ref := ctx.Grant().Identity().VirtualKey(); ref != nil && r.sharedIDs[ref.ID] {
				auth.Pools = []string{source.Pool}
			}
			if explicit, ok := ctx.Value(nativeSessionKey).(string); ok {
				session, err = r.native.SessionHash(auth.SubjectHash, &explicit)
			}
		}
	}
	if errors.Is(err, ErrExpired) {
		return provenanceError(408)
	}
	if err != nil || len(auth.Pools) == 0 {
		return provenanceError(403)
	}
	if !contains(auth.Pools, routed.Pool) {
		return provenanceError(403)
	}
	p := &Principal{authorization: auth, session: session, delegation: proof, source: *source, original: original, routed: routed, deadline: now.Add(60 * time.Second)}
	if proof != nil {
		p.deadline = proof.QueueDeadline()
		_ = json.Unmarshal(proof.OriginalControls(), &p.controls)
	} else if budget, ok := ctx.Value(nativeBudgetKey).(*nativeBudget); ok && budget != nil {
		p.deadline = budget.deadline
		p.controls = budget.controls
		p.controlsInvalid = budget.invalid
	}
	ctx.SetValue(principalKey, p)
	return r.inner.Handle(ctx, req)
}

func gatewayHTTPError(ctx *fasthttp.RequestCtx, status int, code string) {
	ctx.SetStatusCode(status)
	ctx.SetContentType("application/json")
	ctx.SetBodyString(`{"error":{"type":"managed_gateway_error","code":"` + code + `","message":"Gateway channel authorization failed"}}`)
}

// WrapHTTP is installed before routing. It binds signatures to the original
// bytes, then uses the existing OpenAI parser, middleware, Grant and post-hooks.
// Only a small immutable proof is copied into BifrostContext by the transport.
// Native endpoints never accept a delegation token as native service identity.
// A nil runtime reserves the private channel fail-closed; normal paths retain
// their original behavior. No body, image or token is stored in context.
func (r *Runtime) WrapHTTP(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		path := string(ctx.Path())
		gateway := path == GatewayPrefix || strings.HasPrefix(path, GatewayPrefix+"/")
		values := ctx.Request.Header.PeekAll(DelegationHeader)
		if !gateway {
			if len(values) != 0 {
				gatewayHTTPError(ctx, 401, "GATEWAY_CHANNEL_REQUIRED")
				return
			}
			if r != nil && ctx.IsPost() && (strings.HasSuffix(path, "/chat/completions") || strings.HasSuffix(path, "/responses")) {
				budget := &nativeBudget{deadline: time.Now().Add(60 * time.Second), http: true}
				if len(ctx.PostBody()) <= maxInferenceBytes {
					var err error
					budget.controls, err = extractControls(ctx.PostBody())
					budget.invalid = err != nil
					var wire struct {
						Model string `json:"model"`
					}
					if json.Unmarshal(ctx.PostBody(), &wire) == nil && len(wire.Model) <= 513 && strings.TrimSpace(wire.Model) == wire.Model {
						budget.origin = r.nativeHTTPOrigin(wire.Model)
					} else {
						budget.invalid = true
					}
				} else {
					budget.invalid = true
				}
				ctx.SetUserValue(nativeBudgetKey, budget)
				defer ctx.RemoveUserValue(nativeBudgetKey)
				for _, name := range []string{"x-session-id", "x-conversation-id"} {
					sessions := ctx.Request.Header.PeekAll(name)
					if len(sessions) == 0 {
						continue
					}
					value := string(sessions[0])
					if !boundedIdentity(value) {
						gatewayHTTPError(ctx, 400, "INVALID_SESSION")
						return
					}
					for _, other := range sessions[1:] {
						if string(other) != value {
							gatewayHTTPError(ctx, 400, "INVALID_SESSION")
							return
						}
					}
					ctx.SetUserValue(nativeSessionKey, value)
					defer ctx.RemoveUserValue(nativeSessionKey)
					break
				}
			}
			next(ctx)
			return
		}
		api := API(strings.TrimPrefix(path, GatewayPrefix))
		if api != Chat && api != Responses {
			gatewayHTTPError(ctx, 404, "GATEWAY_ROUTE_UNKNOWN")
			return
		}
		if !ctx.IsPost() {
			ctx.Response.Header.Set("Allow", "POST")
			gatewayHTTPError(ctx, 405, "GATEWAY_POST_REQUIRED")
			return
		}
		if r == nil || r.verifier == nil {
			gatewayHTTPError(ctx, 401, "GATEWAY_DELEGATION_REQUIRED")
			return
		}
		if len(ctx.URI().QueryString()) != 0 {
			gatewayHTTPError(ctx, 400, "GATEWAY_QUERY_UNSUPPORTED")
			return
		}
		// The dedicated channel carries only the Gateway's documented protocol
		// headers. Per-request provider/key/compat/async overrides are not signed
		// and cannot change how the original bytes are interpreted.
		unsafeHeader := false
		ctx.Request.Header.VisitAll(func(key, _ []byte) {
			name := strings.ToLower(string(key))
			if strings.HasPrefix(name, "x-bf-") && name != "x-bf-vk" && name != "x-bf-passthrough-extra-params" && name != "x-bf-eh-x-session-id" {
				unsafeHeader = true
			}
		})
		if unsafeHeader {
			gatewayHTTPError(ctx, 400, "GATEWAY_OVERRIDE_UNSUPPORTED")
			return
		}
		if len(ctx.PostBody()) > maxInferenceBytes {
			gatewayHTTPError(ctx, 413, "GATEWAY_BODY_TOO_LARGE")
			return
		}
		tokens := make([]string, len(values))
		for i, v := range values {
			tokens[i] = string(v)
		}
		proof, err := r.verifier.Verify(tokens, string(ctx.Method()), api, ctx.PostBody(), time.Now())
		if err != nil {
			gatewayHTTPError(ctx, 401, "GATEWAY_DELEGATION_REQUIRED")
			return
		}
		var wire struct {
			Model string `json:"model"`
		}
		if json.Unmarshal(ctx.PostBody(), &wire) != nil {
			gatewayHTTPError(ctx, 400, "GATEWAY_BODY_INVALID")
			return
		}
		for _, model := range r.models {
			if model.Alias != proof.claims.SourceAlias {
				continue
			}
			if wire.Model != string(model.Provider)+"/"+model.Model {
				break
			}
			envelope := &gatewayEnvelope{proof: proof, source: model}
			ctx.SetUserValue(gatewayProofKey, envelope)
			defer ctx.RemoveUserValue(gatewayProofKey)
			ctx.Request.URI().SetPath("/openai" + string(api))
			defer ctx.Request.URI().SetPath(path)
			next(ctx)
			// Remove any provider/plugin-supplied imitation. Only our exact final
			// serialized rejection and confirmed original call can carry a token.
			ctx.Response.Header.Del(NoDispatchHeader)
			if envelope.noDispatch.Load() && !ctx.Response.IsBodyStream() {
				if token := r.verifier.signRejection(proof, ctx.Response.StatusCode(), ctx.Response.Body(), time.Now()); token != "" {
					ctx.Response.Header.Set(NoDispatchHeader, token)
				}
			}
			return
		}
		gatewayHTTPError(ctx, 403, "GATEWAY_SOURCE_MISMATCH")
	}
}
