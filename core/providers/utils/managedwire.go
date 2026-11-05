package utils

import "github.com/maximhq/bifrost/core/schemas"

type managedWireAccountingKey struct{}

// EnableManagedWireAccounting is for a detached, deployment-owned decoder
// context only. It marks the exact decoded final chunk for the original core
// post-hook pipeline without running that pipeline twice. Never infer engine
// termination from this accounting marker. Ordinary provider chunks are unchanged.
func EnableManagedWireAccounting(ctx *schemas.BifrostContext) {
	ctx.SetValue(managedWireAccountingKey{}, true)
}
