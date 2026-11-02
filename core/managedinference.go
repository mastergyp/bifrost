package bifrost

import "github.com/maximhq/bifrost/core/schemas"

func managedFailure() *schemas.BifrostError {
	return &schemas.BifrostError{StatusCode: new(503), IsBifrostError: true, AllowFallbacks: new(false), Error: &schemas.ErrorField{Message: "managed inference is unavailable"}}
}
func managedNoFallback(err *schemas.BifrostError) *schemas.BifrostError {
	if err == nil {
		return nil
	}
	copy := *err
	copy.AllowFallbacks = new(false)
	return &copy
}

// Register before pre-hooks or any queue lookup for an owned provider. The phase
// lock covers Add so Shutdown's Wait cannot race a not-yet-registered callback.
func (b *Bifrost) requiresManaged(ctx *schemas.BifrostContext) bool {
	guard, ok := b.managedInference.(schemas.ContextManagedInference)
	return ok && guard.RequiresManaged(ctx)
}

func (b *Bifrost) guardManagedShortcut(ctx *schemas.BifrostContext, req *schemas.BifrostRequest, result *schemas.LLMPluginShortCircuit, registered bool) *schemas.LLMPluginShortCircuit {
	if result == nil || result.Error != nil || !(registered || b.requiresManaged(ctx)) {
		return result
	}
	policy, ok := b.managedInference.(schemas.ManagedShortcutPolicy)
	if !ok {
		return result
	}
	if err := policy.ValidateShortcut(ctx, req, result); err != nil {
		// Cache streams already produced in memory must not block their producer.
		// This does not certify or release any physical engine work. Managed
		// profiles cannot permit pre-hooks that independently execute models.
		drainAbandonedStream(result.Stream)
		return &schemas.LLMPluginShortCircuit{Error: managedNoFallback(err)}
	}
	return result
}

func (b *Bifrost) beginManaged(ctx *schemas.BifrostContext, provider schemas.ModelProvider, typ schemas.RequestType) (func(), *schemas.BifrostError) {
	if b.managedInference == nil {
		return nil, nil
	}
	if b.ctx.Err() != nil {
		return nil, managedFailure()
	}
	if typ == schemas.ListModelsRequest || !(b.managedInference.Owns(provider) || b.requiresManaged(ctx)) {
		return nil, nil
	}
	b.managedMu.RLock()
	defer b.managedMu.RUnlock()
	if b.managedStopping {
		return nil, managedFailure()
	}
	b.managedWG.Add(1)
	return b.managedWG.Done, nil
}
func (b *Bifrost) managedBoundary(ctx *schemas.BifrostContext, req *schemas.BifrostRequest, held *func(), original schemas.ModelProvider) *schemas.LLMPluginShortCircuit {
	if b.managedInference == nil || req == nil || req.RequestType == schemas.ListModelsRequest {
		return nil
	}
	provider, _, _ := req.GetRequestFields()
	owned := b.managedInference.Owns(provider) || b.requiresManaged(ctx)
	// An owned route cannot escape to an unregistered provider in a later hook.
	if b.managedInference.Owns(original) && !owned {
		return &schemas.LLMPluginShortCircuit{Error: managedFailure()}
	}
	if !owned {
		return nil
	}
	if *held == nil {
		done, err := b.beginManaged(ctx, provider, req.RequestType)
		if err != nil {
			return &schemas.LLMPluginShortCircuit{Error: err}
		}
		*held = done
	}
	result := b.managedInference.Handle(ctx, req)
	if result == nil {
		return &schemas.LLMPluginShortCircuit{Error: managedFailure()}
	}
	count := 0
	if result.Response != nil {
		count++
	}
	if result.Stream != nil {
		count++
	}
	if result.Error != nil {
		count++
	}
	if count != 1 {
		return &schemas.LLMPluginShortCircuit{Error: managedFailure()}
	}
	// A stream result on a unary call would otherwise fall through the legacy
	// switch and enqueue provider I/O. Streaming calls must supply their own
	// terminal chunk so the original accounting hooks can observe finality.
	if result.Error == nil && ((IsStreamRequestType(req.RequestType) && result.Stream == nil) || (!IsStreamRequestType(req.RequestType) && result.Response == nil)) {
		return &schemas.LLMPluginShortCircuit{Error: managedFailure()}
	}
	if result.Error != nil {
		copy := *result
		copy.Error = managedNoFallback(result.Error)
		return &copy
	}
	return result
}
func (b *Bifrost) stopManaged() {
	if b.managedInference == nil {
		return
	}
	b.managedMu.Lock()
	first := !b.managedStopping
	b.managedStopping = true
	b.managedMu.Unlock()
	if first {
		b.managedInference.Close()
	}
	// Cancel the core default context, but let a qualified sender retain accepted
	// inference independently. Do not hold providerLifecycleMu while joining: a
	// request registered before the fence may still be doing its queue lookup.
	if b.ctx.Err() == nil && b.cancel != nil {
		b.cancel()
	}
	b.managedWG.Wait()
}
