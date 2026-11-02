package governance

import (
	"reflect"
	"strings"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/grant"
)

type managedFundingKey struct{}

// ManagedFunding is a bounded, short-lived selected-target approval. It keeps
// the original Grant intact and adds the selected target's settled payers to
// the original payers. It does not reserve or debit money, certify an engine
// contract, or select a provider key. Only the configured producer may bind it
// after physical selection and before dispatch; normal governance is unchanged.
type ManagedFunding struct {
	issuer       *GovernancePlugin
	owner        *schemas.BifrostContext
	original     schemas.Grant
	identity     schemas.Identity
	access       schemas.Access
	limits       schemas.Limits
	combined     schemas.Limits
	target       schemas.Route
	requestType  schemas.RequestType
	requestID    string
	billingNonce string
	validUntil   time.Time
}

func (*ManagedFunding) String() string     { return "managed target funding (redacted)" }
func (f *ManagedFunding) GoString() string { return f.String() }

func (f *ManagedFunding) Target() schemas.Route {
	if f == nil {
		return schemas.Route{}
	}
	return f.target
}

func (f *ManagedFunding) RequestType() schemas.RequestType {
	if f == nil {
		return ""
	}
	return f.requestType
}

func managedFundingError(status int, message string) *schemas.BifrostError {
	return &schemas.BifrostError{StatusCode: &status, IsBifrostError: true, AllowFallbacks: new(false), Error: &schemas.ErrorField{Message: message}}
}

func sameFundingHandle(a, b any) bool {
	t := reflect.TypeOf(a)
	return t != nil && t == reflect.TypeOf(b) && t.Comparable() && a == b
}

// CheckManagedTarget must run again after queueing, for the actually selected
// target. Candidate probes do not set rejection flags or change the Grant on
// the original context. The deadline is the original absolute queue deadline.
// This initial managed profile supports the observed keyless private backends;
// named-key execution needs its own registered key/endpoint contract.
func (p *GovernancePlugin) CheckManagedTarget(ctx *schemas.BifrostContext, typ schemas.RequestType, target schemas.Route, deadline time.Time) (*ManagedFunding, *schemas.BifrostError) {
	if p == nil || p.store == nil || p.resolver == nil || ctx == nil || ctx.Grant() == nil ||
		target.Provider == "" || target.Model == "" || len(target.Model) > 256 || len(target.Provider) > 256 ||
		strings.TrimSpace(target.Model) != target.Model || strings.TrimSpace(string(target.Provider)) != string(target.Provider) {
		return nil, managedFundingError(503, "managed target funding is unavailable")
	}
	switch typ {
	case schemas.ChatCompletionRequest, schemas.ChatCompletionStreamRequest, schemas.ResponsesRequest, schemas.ResponsesStreamRequest:
	default:
		return nil, managedFundingError(400, "managed funding request type is unsupported")
	}
	ctx = ctx.Root()
	now := time.Now()
	if !deadline.After(now) || ctx.Err() != nil {
		return nil, managedFundingError(408, "managed target funding deadline exceeded")
	}
	if target.KeyID != "" || bifrost.GetStringFromContext(ctx, schemas.BifrostContextKeyAPIKeyID) != "" {
		return nil, managedFundingError(403, "named provider key is not qualified for managed dispatch")
	}
	g := ctx.Grant()
	identity, access, original := g.Identity(), g.Access(), g.Limits()
	if identity == nil || !identity.Presented() || access == nil || original == nil ||
		!sameFundingHandle(g, g) || !sameFundingHandle(identity, identity) || !sameFundingHandle(access, access) || !sameFundingHandle(original, original) {
		return nil, managedFundingError(403, "managed funding requires the original admitted grant")
	}
	if unusablePermit(access) != nil || !access.IsModelAllowed(string(target.Provider), target.Model) {
		return nil, managedFundingError(403, "selected managed target is not allowed")
	}
	if _, restricted := access.KeysForModel(string(target.Provider), target.Model); restricted {
		return nil, managedFundingError(403, "restricted provider keys do not grant keyless managed execution")
	}
	if spendingChecksSkipped(ctx) || bifrost.GetBoolFromContext(ctx, schemas.BifrostContextKeySkipVirtualKeyUsageTracking) {
		return nil, managedFundingError(403, "managed target funding cannot skip checks or accounting")
	}
	validUntil := now.Add(2 * time.Second)
	if deadline.Before(validUntil) {
		validUntil = deadline
	}
	check := schemas.NewBifrostContext(ctx, validUntil)
	defer check.Cancel()
	budgets, rates, err := p.store.GatherLimits(check, access, target.Provider, target.Model)
	if err != nil {
		return nil, managedFundingError(503, "selected managed target payers could not be resolved")
	}
	// Read the admitted source snapshot; never resolve it again from mutable DB
	// state, replace it with a transit key, or charge shared IDs twice.
	budgets = append(budgets, original.Budgets()...)
	rates = append(rates, original.RateLimits()...)
	if len(budgets) > 256 || len(rates) > 256 {
		return nil, managedFundingError(503, "managed target payers exceed the approved bound")
	}
	combined := grant.NewLimits(budgets, rates)
	decision, checkErr := p.store.CheckRateLimits(check, combined.RateLimits(), nil, nil)
	if checkErr == nil && decision == DecisionAllow {
		decision, checkErr = p.store.CheckBudgets(check, combined.Budgets(), nil)
	}
	if check.Err() != nil || ctx.Err() != nil || !time.Now().Before(validUntil) {
		return nil, managedFundingError(408, "managed target funding deadline exceeded")
	}
	if checkErr != nil || decision != DecisionAllow {
		if decision == DecisionAllow {
			return nil, managedFundingError(503, "selected managed target funding check failed")
		}
		_, denied := p.decide(check, &EvaluationResult{Decision: decision, Reason: reasonFromErr(checkErr, decision)})
		if denied == nil {
			denied = managedFundingError(503, "selected managed target funding was not approved")
		}
		return nil, denied
	}
	return &ManagedFunding{issuer: p, owner: ctx, original: g, identity: identity, access: access, limits: original, combined: combined, target: target, requestType: typ,
		requestID: bifrost.GetStringFromContext(ctx, schemas.BifrostContextKeyRequestID), billingNonce: bifrost.GetStringFromContext(ctx, schemas.BifrostContextKeyBillingNonce), validUntil: validUntil}, nil
}

// Bind transfers this approval to the original accounting context. It is not a
// dispatch receipt: inference can still fail before consuming any tokens. An
// old approval, a changed Grant, another request, or a second selected target
// cannot replace an already bound funding decision.
func (f *ManagedFunding) Bind(ctx *schemas.BifrostContext) *schemas.BifrostError {
	if f == nil || ctx == nil {
		return managedFundingError(403, "managed funding approval is missing")
	}
	ctx = ctx.Root()
	if ctx.Err() != nil || !time.Now().Before(f.validUntil) {
		return managedFundingError(408, "managed funding approval expired before dispatch")
	}
	if ctx != f.owner ||
		!sameFundingHandle(ctx.Grant(), f.original) || !sameFundingHandle(ctx.Grant().Identity(), f.identity) ||
		!sameFundingHandle(ctx.Grant().Access(), f.access) || !sameFundingHandle(ctx.Grant().Limits(), f.limits) ||
		bifrost.GetStringFromContext(ctx, schemas.BifrostContextKeyRequestID) != f.requestID ||
		bifrost.GetStringFromContext(ctx, schemas.BifrostContextKeyBillingNonce) != f.billingNonce {
		return managedFundingError(403, "managed funding approval no longer matches the original request")
	}
	if previous, _ := ctx.Value(managedFundingKey{}).(*ManagedFunding); previous != nil && previous != f && sameFundingHandle(previous.limits, f.limits) {
		return managedFundingError(409, "managed target funding was already bound")
	}
	ctx.SetValue(managedFundingKey{}, f)
	return nil
}

func (p *GovernancePlugin) managedFunding(ctx *schemas.BifrostContext, typ schemas.RequestType) *ManagedFunding {
	f, _ := ctx.Value(managedFundingKey{}).(*ManagedFunding)
	if f == nil || f.issuer != p || f.owner != ctx.Root() || f.requestType != typ ||
		bifrost.GetStringFromContext(ctx, schemas.BifrostContextKeyRequestID) != f.requestID ||
		bifrost.GetStringFromContext(ctx, schemas.BifrostContextKeyBillingNonce) != f.billingNonce {
		return nil
	}
	// Accepted usage may arrive after the caller/deadline is gone. The approved
	// snapshots, not current configuration or a fresh identity, fund that usage.
	return f
}
