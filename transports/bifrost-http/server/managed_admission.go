package server

import (
	"context"
	"os"
	"time"

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
		return funding, nil
	}, logger)
	if err != nil {
		return err
	}
	s.ManagedAdmission = runtime
	return nil
}
