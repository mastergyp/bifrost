package admission

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

// Profile is immutable, operator-owned startup input. It references independent
// machine secrets; it contains no employee token, virtual-key value or URL.
type Profile struct {
	Version                int                    `json:"version"`
	Namespace              string                 `json:"namespace"`
	ContractSHA256         string                 `json:"contract_sha256"`
	Socket                 string                 `json:"socket"`
	SubjectKeyEnv          string                 `json:"subject_key_env"`
	DelegationKeys         []ProfileDelegationKey `json:"delegation_keys"`
	Models                 []ProfileModel         `json:"models"`
	NativeAliases          []ProfileAlias         `json:"native_aliases"`
	SharedLegacyVKIDs      []string               `json:"shared_legacy_vk_ids"`
	DelegatedCredentialIDs []string               `json:"delegated_credential_ids"`
	MaxOperations          int                    `json:"max_operations"`
}
type ProfileDelegationKey struct {
	ID                   string   `json:"id"`
	SecretEnv            string   `json:"secret_env"`
	GatewayVirtualKeyIDs []string `json:"gateway_virtual_key_ids"`
}
type ProfileModel struct {
	Alias    string                `json:"alias"`
	Pool     string                `json:"pool"`
	Provider schemas.ModelProvider `json:"provider"`
	Model    string                `json:"model"`
}
type ProfileAlias struct {
	Provider    schemas.ModelProvider `json:"provider"`
	Model       string                `json:"model"`
	SourceAlias string                `json:"source_alias"`
}

// TargetFunding remains the original governance owner's live target check.
// This constructor never manufactures a Grant or substitutes a new payer.
type TargetFunding func(*schemas.BifrostContext, schemas.RequestType, schemas.Route, time.Time) (SelectedFunding, *schemas.BifrostError)

func readProfile(path string) (Profile, error) {
	var profile Profile
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return profile, ErrRejected
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm()&0022 != 0 || before.Size() > 262144 {
		return profile, ErrRejected
	}
	f, err := os.Open(path)
	if err != nil {
		return profile, ErrRejected
	}
	defer f.Close()
	after, err := f.Stat()
	if err != nil || !os.SameFile(before, after) {
		return profile, ErrRejected
	}
	raw, err := io.ReadAll(io.LimitReader(f, 262145))
	if err != nil || len(raw) > 262144 {
		return profile, ErrRejected
	}
	if uniqueDelegationJSON(json.NewDecoder(bytes.NewReader(raw)), 0) != nil {
		return profile, ErrRejected
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&profile) != nil || decoder.Decode(new(any)) != io.EOF {
		return Profile{}, ErrRejected
	}
	if profile.Version != 1 || !validUUID(profile.Namespace) || !validDigest(profile.ContractSHA256) || profile.MaxOperations < 1 || profile.MaxOperations > 4096 || len(profile.Models) == 0 || len(profile.Models) > 32 || len(profile.NativeAliases) > 128 || len(profile.DelegationKeys) == 0 || len(profile.DelegationKeys) > 8 {
		return Profile{}, ErrRejected
	}
	return profile, nil
}

var profileSecretEnv = regexp.MustCompile(`^BIFROST_MANAGED_[A-Z0-9_]{1,96}$`)

// LoadProfile is the same composition used by the standalone transport. It
// refuses old/non-enforcing controllers BEFORE installing a new request path.
// Policy/health/context are computed by the same fixed Rust registry that owns
// execution; the HTTP transport cannot add a destination or loosen a contract.
func LoadProfile(ctx context.Context, path string, lookup func(string) (string, bool), funding TargetFunding, logger schemas.Logger) (*Runtime, error) {
	config, err := readProfile(path)
	if err != nil || lookup == nil || funding == nil || logger == nil {
		return nil, ErrRejected
	}
	secret := func(name string) ([]byte, error) {
		if !profileSecretEnv.MatchString(name) {
			return nil, ErrRejected
		}
		value, ok := lookup(name)
		if !ok || len(value) < 32 || len(value) > 4096 {
			return nil, ErrRejected
		}
		return []byte(value), nil
	}
	subject, err := secret(config.SubjectKeyEnv)
	if err != nil {
		return nil, ErrRejected
	}
	native, err := NewNativeAuthorizer(subject, config.DelegatedCredentialIDs)
	if err != nil {
		return nil, ErrRejected
	}
	keys := make([]DelegationKey, 0, len(config.DelegationKeys))
	seenRefs := map[string]bool{config.SubjectKeyEnv: true}
	for _, key := range config.DelegationKeys {
		if seenRefs[key.SecretEnv] {
			return nil, ErrRejected
		}
		seenRefs[key.SecretEnv] = true
		value, e := secret(key.SecretEnv)
		if e != nil {
			return nil, ErrRejected
		}
		keys = append(keys, DelegationKey{ID: key.ID, Secret: value, GatewayVirtualKeyIDs: key.GatewayVirtualKeyIDs})
	}
	verifier, err := NewDelegationVerifier(keys)
	if err != nil {
		return nil, ErrRejected
	}
	client, err := New(config.Socket)
	if err != nil {
		return nil, ErrRejected
	}
	client.authority = &controlAuthority{namespace: config.Namespace, contract: config.ContractSHA256}
	installed := false
	defer func() {
		if !installed {
			client.Close()
		}
	}()
	if err = client.checkPreparation(ctx); err != nil {
		return nil, err
	}
	models := make([]RuntimeModel, 0, len(config.Models))
	providers := []schemas.ModelProvider{}
	seenProviders := map[schemas.ModelProvider]bool{}
	targets := map[string]ModelTarget{}
	for _, model := range config.Models {
		target := ModelTarget{Pool: model.Pool, Provider: model.Provider, Model: model.Model}
		if previous, ok := targets[target.Pool]; ok && previous != target {
			return nil, ErrRejected
		}
		targets[target.Pool] = target
		models = append(models, RuntimeModel{Alias: model.Alias, ModelTarget: target})
		if !seenProviders[model.Provider] {
			seenProviders[model.Provider] = true
			providers = append(providers, model.Provider)
		}
	}
	policy := &preparationPolicy{client: client, targets: targets, funding: funding}
	producer, err := NewProducer(client, policy, providers, config.MaxOperations, logger)
	if err != nil {
		return nil, err
	}
	aliases := make([]NativeModelAlias, 0, len(config.NativeAliases))
	for _, a := range config.NativeAliases {
		aliases = append(aliases, NativeModelAlias{Provider: a.Provider, Model: a.Model, SourceAlias: a.SourceAlias})
	}
	runtime, err := NewRuntime(producer, native, verifier, models, config.SharedLegacyVKIDs, aliases...)
	if err != nil {
		producer.Close()
		return nil, err
	}
	installed = true
	return runtime, nil
}

type preparationPolicy struct {
	client  *Client
	targets map[string]ModelTarget
	funding TargetFunding
}

func (p *preparationPolicy) Prepare(ctx *schemas.BifrostContext, input FrozenInput, principal *Principal) (Preparation, error) {
	if principal == nil || len(input.body) == 0 || len(input.body) > maxInferenceBytes {
		return Preparation{}, ErrRejected
	}
	allowed := principal.Authorization().Pools
	request := struct {
		Protocol     int             `json:"protocol"`
		API          API             `json:"api"`
		Alias        string          `json:"alias"`
		AllowedPools []string        `json:"allowed_pools"`
		MaxContext   uint32          `json:"max_context_tokens"`
		MaxOutput    uint32          `json:"max_output_tokens"`
		Body         json.RawMessage `json:"body"`
	}{Protocol: 1, API: input.api, Alias: principal.source.Alias, AllowedPools: allowed, Body: input.body}
	if principal.delegation != nil {
		request.MaxContext = principal.delegation.claims.MaxContextTokens
		request.MaxOutput = principal.delegation.claims.MaxOutputTokens
	}
	raw, err := json.Marshal(request)
	if err != nil || len(raw) > maxInferenceBytes+32768 {
		return Preparation{}, ErrRejected
	}
	planning, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(planning, http.MethodPost, "http://localhost/v1/prepare", bytes.NewReader(raw))
	if err != nil {
		return Preparation{}, ErrRejected
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	p.client.setAuthority(httpRequest.Header.Set)
	response, err := p.client.data.Do(httpRequest)
	if err != nil {
		return Preparation{}, ErrUnavailable
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 16385))
	if err != nil || len(body) > 16384 {
		return Preparation{}, ErrUnavailable
	}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode >= 500 {
			return Preparation{}, ErrUnavailable
		}
		return Preparation{}, ErrRejected
	}
	var reply struct {
		Protocol       int      `json:"protocol"`
		Namespace      string   `json:"namespace"`
		ContractSHA256 string   `json:"contract_sha256"`
		Pools          []string `json:"pools"`
		HealthyEngines []string `json:"healthy_engines"`
		Cost           uint64   `json:"cost"`
		NativeWeight   uint32   `json:"native_weight"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if uniqueDelegationJSON(json.NewDecoder(bytes.NewReader(body)), 0) != nil || decoder.Decode(&reply) != nil || decoder.Decode(new(any)) != io.EOF || reply.Protocol != 1 || p.client.authority == nil || reply.Namespace != p.client.authority.namespace || reply.ContractSHA256 != p.client.authority.contract || len(reply.Pools) == 0 || len(reply.Pools) > 32 || len(reply.HealthyEngines) == 0 || len(reply.HealthyEngines) > 32 || reply.Cost == 0 || reply.Cost > 1_000_000_000_000 || reply.NativeWeight < 1 || reply.NativeWeight > 1000 {
		return Preparation{}, ErrRejected
	}
	seen := map[string]bool{}
	for _, pool := range reply.Pools {
		if !contains(allowed, pool) || p.targets[pool].Pool == "" || seen[pool] {
			return Preparation{}, ErrRejected
		}
		seen[pool] = true
	}
	seen = map[string]bool{}
	for _, engine := range reply.HealthyEngines {
		if !boundedIdentity(engine) || seen[engine] {
			return Preparation{}, ErrRejected
		}
		seen[engine] = true
	}
	return Preparation{Pools: reply.Pools, HealthyEngines: reply.HealthyEngines, Cost: reply.Cost, NativeWeight: reply.NativeWeight}, nil
}
func (p *preparationPolicy) AuthorizeSelected(ctx *schemas.BifrostContext, principal *Principal, lease Lease, typ schemas.RequestType) (Selection, *schemas.BifrostError) {
	target, ok := p.targets[lease.Pool]
	if !ok || principal == nil || !contains(principal.authorization.Pools, lease.Pool) {
		return Selection{}, producerError(403, "selected pool exceeds the configured original authority").Error
	}
	approval, err := p.funding(ctx, typ, schemas.Route{Provider: target.Provider, Model: target.Model}, principal.QueueDeadline())
	if err != nil {
		return Selection{}, err
	}
	return Selection{Target: target, Funding: approval}, nil
}
func (c *Client) checkPreparation(ctx context.Context) error {
	var health struct {
		Protocol                 int    `json:"protocol"`
		State                    string `json:"state"`
		Enforcement              string `json:"routing_enforcement"`
		AbsoluteDeadline         bool   `json:"absolute_queue_deadline"`
		PreparationPolicyVersion int    `json:"preparation_policy_version"`
		Namespace                string `json:"namespace"`
		ContractSHA256           string `json:"contract_sha256"`
	}
	if err := c.call(ctx, "health", nil, &health); err != nil {
		return err
	}
	if health.Protocol != 1 || health.State != "ready" || health.Enforcement != "enabled" || !health.AbsoluteDeadline || health.PreparationPolicyVersion != 1 || c.authority == nil || health.Namespace != c.authority.namespace || health.ContractSHA256 != c.authority.contract {
		return ErrNotEnforced
	}
	return nil
}
