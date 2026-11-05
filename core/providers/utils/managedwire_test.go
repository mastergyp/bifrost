package utils

import (
	"context"
	"encoding/json"
	"github.com/maximhq/bifrost/core/schemas"
	"strings"
	"testing"
)

func TestManagedWireFinalAccountingIsOptInAndNotSerialized(t *testing.T) {
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	ctx.SetValue(schemas.BifrostContextKeyStreamEndIndicator, true)
	response := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{ID: "owned-final", Usage: &schemas.BifrostLLMUsage{TotalTokens: 7}}}
	if BuildClientStreamChunk(ctx, response, nil).FinalForAccounting {
		t.Fatal("ordinary provider wire acquired managed finality")
	}
	EnableManagedWireAccounting(ctx)
	chunk := BuildClientStreamChunk(ctx, response, nil)
	if !chunk.FinalForAccounting {
		t.Fatal("owned decoder final chunk did not reach original accounting")
	}
	data, _ := json.Marshal(chunk)
	if strings.Contains(string(data), "FinalForAccounting") || strings.Contains(string(data), "final_for_accounting") {
		t.Fatal("internal accounting marker leaked into client payload")
	}
	ctx.SetValue(schemas.BifrostContextKeyStreamEndIndicator, false)
	if BuildClientStreamChunk(ctx, response, nil).FinalForAccounting {
		t.Fatal("intermediate managed chunk marked final")
	}
}
