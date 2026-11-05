package governance

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	configstoreTables "github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/framework/modelcatalog"
	"github.com/maximhq/bifrost/framework/modelcatalog/datasheet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newOfflinePricingCatalog(t *testing.T) *modelcatalog.ModelCatalog {
	t.Helper()
	abs, err := filepath.Abs("../../framework/modelcatalog/datasheet/testdata/pricing.json")
	require.NoError(t, err)
	ds := datasheet.New(nil, NewMockLogger(), datasheet.Config{URL: "file://" + abs})
	require.NoError(t, ds.LoadFromURLIntoMemory(context.Background()))
	return modelcatalog.NewTestCatalogWithDatasheet(ds)
}

// newWarmupBudgetFixture wires a plugin over a store whose "openai" provider
// carries a provider-level budget — the admin-owned ledger warmup embeds are
// attributed to when count_toward_budgets is on.
func newWarmupBudgetFixture(t *testing.T) (*GovernancePlugin, GovernanceStore) {
	t.Helper()
	logger := NewMockLogger()
	budget := buildBudgetWithUsage("provider-budget", 1000.0, 0.0, "1d")
	budgetID := budget.ID
	store, err := NewLocalGovernanceStore(context.Background(), logger, nil, &configstore.GovernanceConfig{
		Budgets:   []configstoreTables.TableBudget{*budget},
		Providers: []configstoreTables.TableProvider{{Name: "openai", BudgetID: &budgetID}},
	}, nil, nil)
	require.NoError(t, err)
	plugin := &GovernancePlugin{
		ctx:          context.Background(),
		store:        store,
		modelCatalog: newOfflinePricingCatalog(t),
		logger:       logger,
	}
	return plugin, store
}

func TestAttributeRoutingEmbeddingCostBillsProviderBudget(t *testing.T) {
	plugin, store := newWarmupBudgetFixture(t)

	plugin.AttributeRoutingEmbeddingCost("openai", "text-embedding-3-small", 1000)

	// 1000 tokens × $0.00000002/token (text-embedding-3-small in testdata).
	usage := store.GetGovernanceData(context.Background()).Budgets["provider-budget"].CurrentUsage
	assert.InDelta(t, 0.00002, usage, 1e-12)
}

func TestAttributeRoutingEmbeddingCostWithoutStoreOrCatalog(t *testing.T) {
	// A plugin whose catalog or store never got wired must not panic; the
	// usage is simply unattributable.
	(&GovernancePlugin{}).AttributeRoutingEmbeddingCost("openai", "text-embedding-3-small", 1000)
}

func TestPostHookWorkerAddsRoutingCostToProviderUsageOnError(t *testing.T) {
	fixture := newAccountingFixture(t)
	plugin := &GovernancePlugin{
		ctx:          context.Background(),
		tracker:      fixture.tracker,
		modelCatalog: newOfflinePricingCatalog(t),
	}

	provider, model, routingTokens := "openai", "text-embedding-3-small", 13
	routingMetadata := &schemas.BifrostRoutingMetadata{
		Calls: []schemas.BifrostRoutingCall{{
			ProviderUsed:       &provider,
			ModelUsed:          &model,
			InputTokens:        &routingTokens,
			CountTowardBudgets: true,
		}},
	}
	billedUsage := &schemas.BifrostLLMUsage{
		PromptTokens:     100,
		CompletionTokens: 50,
		TotalTokens:      150,
	}
	bifrostErr := &schemas.BifrostError{
		Error: &schemas.ErrorField{Message: "provider failed"},
		ExtraFields: schemas.BifrostErrorExtraFields{
			BilledUsage: billedUsage,
		},
	}

	settled := settleLimits(fixture.store, "sk-bf-acct", schemas.OpenAI, "gpt-4o", &UsageUpdate{})
	update := plugin.prepareUsageUpdate(nil, bifrostErr, schemas.OpenAI, "gpt-4o", schemas.ChatCompletionRequest, "routing-error", "", false, 0, nil, settled.Budgets, settled.RateLimits, routingMetadata)

	require.NotNil(t, update)
	plugin.tracker.UpdateUsage(plugin.ctx, update)

	// gpt-4o testdata: 2.5e-6 input, 1e-5 output. Routing embedding:
	// text-embedding-3-small at 2e-8/input token. Both calls are billable.
	want := float64(100)*2.5e-6 + float64(50)*1e-5 + float64(routingTokens)*2e-8
	// The prepared update goes to the tracker, which the plugin treats as
	// an asynchronous queue. Poll for the settled total instead of sleeping for a
	// fixed interval that is either wasted time or, if the tracker ever moves the
	// work off the caller's goroutine, too short.
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.InDelta(c, want, fixture.cost(), 1e-12)
	}, time.Second, 10*time.Millisecond)
}

// The async tracker must own scalar accounting, never the response that core or
// another post-hook is about to mutate after PostLLMHook returns.
func TestPreparedUsageOwnsCostAndTokensBeforeResponseMutation(t *testing.T) {
	plugin := &GovernancePlugin{modelCatalog: newOfflinePricingCatalog(t)}
	payload := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
		Model: "gpt-4o",
		Usage: &schemas.BifrostLLMUsage{PromptTokens: 5, CompletionTokens: 2, TotalTokens: 7},
	}}
	payload.PopulateExtraFields(schemas.ChatCompletionRequest, schemas.OpenAI, "gpt-4o", "gpt-4o")
	update := plugin.prepareUsageUpdate(payload, nil, schemas.OpenAI, "gpt-4o", schemas.ChatCompletionRequest, "owned-snapshot", "", false, 0, nil, nil, nil, nil)
	require.NotNil(t, update)
	want := 5*2.5e-6 + 2*1e-5
	payload.ChatResponse.Usage.TotalTokens = 999
	payload.ChatResponse.Usage.PromptTokens = 999
	payload.PopulateExtraFields(schemas.ChatCompletionRequest, schemas.Anthropic, "replaced", "replaced")
	assert.Equal(t, int64(7), update.TokensUsed)
	assert.InDelta(t, want, update.Cost, 1e-12)
	assert.Equal(t, "owned-snapshot", update.RequestID)
	assert.Nil(t, plugin.prepareUsageUpdate(payload, nil, schemas.OpenAI, "gpt-4o", schemas.ChatCompletionStreamRequest, "not-final", "", false, 0, nil, nil, nil, nil))
}

func TestPreparedUsageContainsPricingPanic(t *testing.T) {
	plugin := &GovernancePlugin{modelCatalog: &modelcatalog.ModelCatalog{}, logger: NewMockLogger()}
	payload := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{Model: "gpt-4o", Usage: &schemas.BifrostLLMUsage{PromptTokens: 5, TotalTokens: 5}}}
	payload.PopulateExtraFields(schemas.ChatCompletionRequest, schemas.OpenAI, "gpt-4o", "gpt-4o")
	assert.NotPanics(t, func() {
		assert.Nil(t, plugin.prepareUsageUpdate(payload, nil, schemas.OpenAI, "gpt-4o", schemas.ChatCompletionRequest, "contained-pricing-failure", "", false, 0, nil, nil, nil, nil))
	})
}
