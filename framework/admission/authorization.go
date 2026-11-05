package admission

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"unicode"

	"github.com/maximhq/bifrost/core/schemas"
)

var (
	ErrGrantUnresolved    = errors.New("managed inference requires a resolved authenticated grant")
	ErrSourceDenied       = errors.New("managed source model is not authorized")
	ErrDelegationRequired = errors.New("delegated gateway credential requires verified original caller intent")
)

// ModelTarget is deployment input, never a caller-supplied destination URL. This
// is a model-access intersection only: it does not approve quality, parameters,
// keys, target budgets or health; those remain mandatory separate checks.
type ModelTarget struct {
	Pool     string
	Provider schemas.ModelProvider
	Model    string
}

type NativeAuthorization struct {
	SubjectHash string
	Pools       []string
	// Preserve the original admitted limits rather than replacing or resolving
	// them from current mutable DB configuration. Never serialize this handle.
	OriginalLimits schemas.Limits `json:"-"`
}

type NativeAuthorizer struct {
	secret    []byte
	delegated map[string]struct{}
}

func NewNativeAuthorizer(subjectSecret []byte, delegatedCredentialIDs []string) (*NativeAuthorizer, error) {
	if len(subjectSecret) < 32 || len(subjectSecret) > 4096 || len(delegatedCredentialIDs) > 128 {
		return nil, ErrRejected
	}
	a := &NativeAuthorizer{secret: append([]byte(nil), subjectSecret...), delegated: make(map[string]struct{}, len(delegatedCredentialIDs))}
	for _, id := range delegatedCredentialIDs {
		if !boundedIdentity(id) {
			return nil, ErrRejected
		}
		a.delegated[id] = struct{}{}
	}
	return a, nil
}
func boundedIdentity(v string) bool {
	return len(v) > 0 && len(v) <= 256 && strings.TrimSpace(v) == v && strings.IndexFunc(v, unicode.IsControl) < 0
}
func validTarget(t ModelTarget) bool {
	return boundedIdentity(t.Pool) && boundedIdentity(string(t.Provider)) && boundedIdentity(t.Model)
}
func (a *NativeAuthorizer) digest(parts ...string) string {
	h := hmac.New(sha256.New, a.secret)
	for _, v := range parts {
		_, _ = h.Write([]byte(v))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Authorize reads the resolved Grant after governance, never payload.user, raw
// keys, incoming identity headers or a username. It does not mutate the Grant.
// A configured transit VK cannot impersonate its original Gateway user here:
// it must take the separate verified delegation path, even if it grants all GPUs.
// Nil Access/Limits are unsettled and rejected for this opt-in private path;
// this does not change Bifrost's general nil-Access/unrestricted semantics.
func (a *NativeAuthorizer) Authorize(ctx *schemas.BifrostContext, source ModelTarget, candidates []ModelTarget) (NativeAuthorization, error) {
	if a == nil || len(a.secret) < 32 || ctx == nil || !validTarget(source) || len(candidates) > 32 {
		return NativeAuthorization{}, ErrRejected
	}
	g := ctx.Grant()
	if g == nil {
		return NativeAuthorization{}, ErrGrantUnresolved
	}
	identity, access, limits := g.Identity(), g.Access(), g.Limits()
	if identity == nil || !identity.Presented() || access == nil || limits == nil {
		return NativeAuthorization{}, ErrGrantUnresolved
	}
	vk, user := "", ""
	if ref := identity.VirtualKey(); ref != nil {
		vk = ref.ID
		if !boundedIdentity(vk) {
			return NativeAuthorization{}, ErrGrantUnresolved
		}
	}
	if ref := identity.User(); ref != nil {
		user = ref.ID
		if !boundedIdentity(user) {
			return NativeAuthorization{}, ErrGrantUnresolved
		}
	}
	if vk == "" && user == "" {
		return NativeAuthorization{}, ErrGrantUnresolved
	}
	if _, delegated := a.delegated[vk]; delegated {
		return NativeAuthorization{}, ErrDelegationRequired
	}
	if !access.IsModelAllowed(string(source.Provider), source.Model) {
		return NativeAuthorization{}, ErrSourceDenied
	}
	result := NativeAuthorization{SubjectHash: a.digest("sw-native-principal-v1", vk, user), OriginalLimits: limits}
	seen := make(map[string]struct{}, len(candidates))
	for _, target := range candidates {
		if !validTarget(target) {
			return NativeAuthorization{}, ErrRejected
		}
		if _, duplicate := seen[target.Pool]; duplicate {
			return NativeAuthorization{}, ErrRejected
		}
		seen[target.Pool] = struct{}{}
		if access.IsModelAllowed(string(target.Provider), target.Model) {
			result.Pools = append(result.Pools, target.Pool)
		}
	}
	return result, nil
}

// SessionHash is optional and explicitly supplied. Missing means home-only at
// physical admission. An employee/key digest, caller user field, body hash or
// x-request-id must never be substituted as a conversation identifier.
func (a *NativeAuthorizer) SessionHash(subjectHash string, explicitSession *string) (*string, error) {
	if a == nil || len(a.secret) < 32 || !validDigest(subjectHash) {
		return nil, ErrRejected
	}
	if explicitSession == nil {
		return nil, nil
	}
	if !boundedIdentity(*explicitSession) {
		return nil, ErrRejected
	}
	value := a.digest("sw-native-session-v1", subjectHash, *explicitSession)
	return &value, nil
}
