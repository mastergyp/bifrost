package server

import (
	"context"
	"os"
	"time"

	wire "github.com/maximhq/bifrost/core/providers/utils"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/admission"
	"github.com/maximhq/bifrost/plugins/governance"
)

type managedTargetFunder interface {
	CheckManagedTarget(*schemas.BifrostContext, schemas.RequestType, schemas.Route, time.Time) (*governance.ManagedFunding, *schemas.BifrostError)
}

// Startup-only, operator-owned config; no admin/UI write can turn a shared
// virtual key into trusted employee provenance. Unset preserves native behavior.
func (s *BifrostHTTPServer) configureManagedAdmission(ctx context.Context) error {
	path, enabled := os.LookupEnv("BIFROST_MANAGED_ADMISSION_CONFIG")
	if !enabled {
		return nil
	}
	if path == "" || s.ManagedAdmission != nil || s.Config == nil {
		return admission.ErrRejected
	}
	original, err := s.getGovernancePlugin()
	if err != nil {
		return admission.ErrGrantUnresolved
	}
	funder, ok := original.(managedTargetFunder)
	if !ok {
		return admission.ErrGrantUnresolved
	}
	runtime, err := admission.LoadProfile(ctx, path, os.LookupEnv, func(c *schemas.BifrostContext, t schemas.RequestType, r schemas.Route, d time.Time) (admission.SelectedFunding, *schemas.BifrostError) {
		funding, denied := funder.CheckManagedTarget(c, t, r, d)
		if denied != nil {
			return nil, denied
		}
		return s.withManagedStreamPolicy(funding, r.Provider)
	}, logger)
	if err != nil {
		return err
	}
	s.ManagedAdmission = runtime
	return nil
}

// Preserve the original governance handle; attach only the selected provider's
// non-secret network scalar. Neither request headers nor a source model supply it.
type managedStreamFunding struct {
	admission.SelectedFunding
	idle time.Duration
}

func (f *managedStreamFunding) StreamIdleTimeout() time.Duration { return f.idle }

func (s *BifrostHTTPServer) withManagedStreamPolicy(funding admission.SelectedFunding, provider schemas.ModelProvider) (admission.SelectedFunding, *schemas.BifrostError) {
	if funding == nil || s.Config == nil {
		return nil, &schemas.BifrostError{StatusCode: new(503), IsBifrostError: true, AllowFallbacks: new(false), Error: &schemas.ErrorField{Message: "managed selected stream configuration is unavailable"}}
	}
	config, err := s.Config.GetProviderConfigRaw(provider)
	if err != nil || config == nil {
		return nil, &schemas.BifrostError{StatusCode: new(503), IsBifrostError: true, AllowFallbacks: new(false), Error: &schemas.ErrorField{Message: "managed selected stream configuration is unavailable"}}
	}
	idle := wire.DefaultStreamIdleTimeout
	if config.NetworkConfig != nil && config.NetworkConfig.StreamIdleTimeoutInSeconds > 0 {
		// The producer's existing20-minute owned operation deadline remains
		// authoritative even if an operator configures a longer idle period.
		seconds := min(config.NetworkConfig.StreamIdleTimeoutInSeconds, 20*60)
		idle = time.Duration(seconds) * time.Second
	}
	return &managedStreamFunding{SelectedFunding: funding, idle: idle}, nil
}
