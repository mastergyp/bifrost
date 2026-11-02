package admission

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

const DelegationHeader = "X-SW-Gateway-Delegation-Token"
const delegationDomain = "sw-gateway-delegation-v1\x00"

var controlFields = map[string]bool{"reasoning_effort": true, "reasoning": true, "thinking": true, "enable_thinking": true, "chat_template_kwargs": true, "temperature": true, "top_p": true, "top_k": true, "min_p": true, "presence_penalty": true, "frequency_penalty": true, "repetition_penalty": true, "seed": true, "max_tokens": true, "max_completion_tokens": true, "max_output_tokens": true}

type delegationClaims struct {
	Version          uint32          `json:"version"`
	KeyID            string          `json:"key_id"`
	RootID           string          `json:"root_id"`
	CallID           string          `json:"call_id"`
	SubjectHash      string          `json:"subject_hash"`
	SessionHash      *string         `json:"session_hash"`
	SourceAlias      string          `json:"source_alias"`
	AllowedAliases   []string        `json:"allowed_aliases"`
	Purpose          string          `json:"purpose"`
	API              string          `json:"api"`
	BodyHash         string          `json:"body_hash"`
	IssuedAtMS       uint64          `json:"issued_at_ms"`
	ExpiresAtMS      uint64          `json:"expires_at_ms"`
	QueueDeadlineMS  uint64          `json:"queue_deadline_ms"`
	Weight           uint32          `json:"weight"`
	MaxContextTokens uint32          `json:"max_context_tokens"`
	MaxOutputTokens  uint32          `json:"max_output_tokens"`
	OriginalControls json.RawMessage `json:"original_controls"`
}

// Key material is copied. Caller IDs are stable resolved VK row IDs, not key
// values, request headers or an employee ID supplied by the forwarding caller.
type DelegationKey struct {
	ID                   string
	Secret               []byte `json:"-"`
	GatewayVirtualKeyIDs []string
}
type delegationKey struct {
	secret  []byte
	callers map[string]bool
}
type DelegationVerifier struct{ keys map[string]delegationKey }

// VerifiedDelegation cannot be constructed from caller fields outside this
// package. It is request-local, bounded metadata, never a copy of model input.
// Signature verification is NOT replay consumption: use CallID for the durable
// one-shot operation/dispatch CAS before I/O, including after retries/restarts.
type VerifiedDelegation struct {
	claims  delegationClaims
	callers map[string]bool
}
type DelegationTarget struct {
	Alias string
	ModelTarget
}

func delegationID(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}
func NewDelegationVerifier(keys []DelegationKey) (*DelegationVerifier, error) {
	if len(keys) == 0 || len(keys) > 8 {
		return nil, ErrRejected
	}
	v := &DelegationVerifier{keys: make(map[string]delegationKey, len(keys))}
	for _, k := range keys {
		if !delegationID(k.ID) || len(k.Secret) < 32 || len(k.Secret) > 4096 || len(k.GatewayVirtualKeyIDs) == 0 || len(k.GatewayVirtualKeyIDs) > 32 {
			return nil, ErrRejected
		}
		if _, ok := v.keys[k.ID]; ok {
			return nil, ErrRejected
		}
		callers := make(map[string]bool, len(k.GatewayVirtualKeyIDs))
		for _, id := range k.GatewayVirtualKeyIDs {
			if !boundedIdentity(id) || callers[id] {
				return nil, ErrRejected
			}
			callers[id] = true
		}
		v.keys[k.ID] = delegationKey{secret: bytes.Clone(k.Secret), callers: callers}
	}
	return v, nil
}
func decodeDelegationPart(part string) ([]byte, error) {
	if strings.IndexFunc(part, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_')
	}) >= 0 {
		return nil, ErrRejected
	}
	b, e := base64.RawURLEncoding.Strict().DecodeString(part)
	if e != nil || base64.RawURLEncoding.EncodeToString(b) != part {
		return nil, ErrRejected
	}
	return b, nil
}

// Reject duplicate members rather than letting two languages interpret a signed
// object differently. The header is capped before any JSON/base64 work.
func uniqueDelegationJSON(d *json.Decoder, depth int) error {
	if depth > 64 {
		return ErrRejected
	}
	t, e := d.Token()
	if e != nil {
		return ErrRejected
	}
	if delim, ok := t.(json.Delim); ok {
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, e := d.Token()
				if e != nil {
					return ErrRejected
				}
				s, ok := key.(string)
				if !ok || seen[s] {
					return ErrRejected
				}
				seen[s] = true
				if uniqueDelegationJSON(d, depth+1) != nil {
					return ErrRejected
				}
			}
			t, e = d.Token()
			if e != nil || t != json.Delim('}') {
				return ErrRejected
			}
		case '[':
			for d.More() {
				if uniqueDelegationJSON(d, depth+1) != nil {
					return ErrRejected
				}
			}
			t, e = d.Token()
			if e != nil || t != json.Delim(']') {
				return ErrRejected
			}
		default:
			return ErrRejected
		}
	}
	return nil
}
func (v *DelegationVerifier) Verify(values []string, method string, api API, body []byte, now time.Time) (*VerifiedDelegation, error) {
	if method != "POST" || v == nil || len(v.keys) == 0 || len(values) != 1 || len(values[0]) > 8192 || len(values[0]) < 16 || now.UnixMilli() < 0 {
		return nil, ErrDelegationRequired
	}
	parts := strings.Split(values[0], ".")
	if len(parts) != 3 || parts[0] != "swgd1" {
		return nil, ErrDelegationRequired
	}
	payload, e := decodeDelegationPart(parts[1])
	if e != nil {
		return nil, ErrDelegationRequired
	}
	sig, e := decodeDelegationPart(parts[2])
	if e != nil || len(sig) != 32 {
		return nil, ErrDelegationRequired
	}
	if uniqueDelegationJSON(json.NewDecoder(bytes.NewReader(payload)), 0) != nil {
		return nil, ErrDelegationRequired
	}
	var c delegationClaims
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil {
		return nil, ErrDelegationRequired
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, ErrDelegationRequired
	}
	key, ok := v.keys[c.KeyID]
	if !ok {
		return nil, ErrDelegationRequired
	}
	h := hmac.New(sha256.New, key.secret)
	_, _ = h.Write([]byte(delegationDomain))
	_, _ = h.Write(payload)
	if !hmac.Equal(sig, h.Sum(nil)) {
		return nil, ErrDelegationRequired
	}
	n := uint64(now.UnixMilli())
	bodyHash, e := RequestDigest(api, body)
	if e != nil || c.Version != 1 || !validUUID(c.RootID) || !validUUID(c.CallID) || !validDigest(c.SubjectHash) || c.SessionHash != nil && !validDigest(*c.SessionHash) || !delegationID(c.SourceAlias) || len(c.AllowedAliases) == 0 || len(c.AllowedAliases) > 32 || c.API != string(api) || c.BodyHash != bodyHash || c.IssuedAtMS > n+2000 || c.IssuedAtMS > c.ExpiresAtMS || c.ExpiresAtMS <= n || c.ExpiresAtMS-c.IssuedAtMS > 60000 || c.QueueDeadlineMS <= n || c.QueueDeadlineMS > c.ExpiresAtMS || c.QueueDeadlineMS > c.IssuedAtMS+60000 || c.QueueDeadlineMS > n+60000 {
		return nil, ErrDelegationRequired
	}
	if c.Weight == 0 || c.Weight > 1000 || c.MaxOutputTokens == 0 || c.MaxContextTokens < c.MaxOutputTokens {
		return nil, ErrDelegationRequired
	}
	if c.Purpose != "main" && c.Purpose != "image_observation" && c.Purpose != "vision_bridge" {
		return nil, ErrDelegationRequired
	}
	for i, a := range c.AllowedAliases {
		if !delegationID(a) || i > 0 && c.AllowedAliases[i-1] >= a {
			return nil, ErrDelegationRequired
		}
	}
	if !contains(c.AllowedAliases, c.SourceAlias) || c.Purpose != "main" && (len(c.AllowedAliases) != 1 || c.AllowedAliases[0] != c.SourceAlias) {
		return nil, ErrDelegationRequired
	}
	var controls map[string]json.RawMessage
	if len(c.OriginalControls) > 3072 || json.Unmarshal(c.OriginalControls, &controls) != nil || controls == nil {
		return nil, ErrDelegationRequired
	}
	for name := range controls {
		if !controlFields[name] {
			return nil, ErrDelegationRequired
		}
	}
	return &VerifiedDelegation{claims: c, callers: key.callers}, nil
}
func (v *VerifiedDelegation) CallID() string {
	if v == nil {
		return ""
	}
	return v.claims.CallID
}
func (v *VerifiedDelegation) RootID() string {
	if v == nil {
		return ""
	}
	return v.claims.RootID
}
func (v *VerifiedDelegation) Purpose() string {
	if v == nil {
		return ""
	}
	return v.claims.Purpose
}
func (v *VerifiedDelegation) QueueDeadline() time.Time {
	if v == nil {
		return time.Time{}
	}
	return time.UnixMilli(int64(v.claims.QueueDeadlineMS))
}
func (v *VerifiedDelegation) SessionHash() *string {
	if v == nil || v.claims.SessionHash == nil {
		return nil
	}
	s := *v.claims.SessionHash
	return &s
}
func (v *VerifiedDelegation) OriginalControls() []byte {
	if v == nil {
		return nil
	}
	return bytes.Clone(v.claims.OriginalControls)
}

// Authorize is called after original governance has resolved and admitted its
// grant. The signed employee scope intersects the existing transit VK scope;
// neither the service VK alone nor a valid signature grants arbitrary models.
// Target financial/key/capability checks remain separate mandatory pre-I/O work.
func (v *VerifiedDelegation) Authorize(ctx *schemas.BifrostContext, source DelegationTarget, targets []DelegationTarget, now time.Time) (NativeAuthorization, error) {
	if v == nil || ctx == nil || !validTarget(source.ModelTarget) || source.Alias != v.claims.SourceAlias || len(targets) > 32 {
		return NativeAuthorization{}, ErrDelegationRequired
	}
	if now.UnixMilli() < 0 || v.claims.ExpiresAtMS <= uint64(now.UnixMilli()) || v.claims.QueueDeadlineMS <= uint64(now.UnixMilli()) {
		return NativeAuthorization{}, ErrExpired
	}
	g := ctx.Grant()
	if g == nil || g.Identity() == nil || g.Access() == nil || g.Limits() == nil || !g.Identity().Presented() {
		return NativeAuthorization{}, ErrGrantUnresolved
	}
	vk := g.Identity().VirtualKey()
	if vk == nil || !v.callers[vk.ID] {
		return NativeAuthorization{}, ErrDelegationRequired
	}
	if !g.Access().IsModelAllowed(string(source.Provider), source.Model) {
		return NativeAuthorization{}, ErrSourceDenied
	}
	auth := NativeAuthorization{SubjectHash: v.claims.SubjectHash, OriginalLimits: g.Limits()}
	seen := map[string]bool{}
	aliases := map[string]bool{}
	for _, t := range targets {
		if !validTarget(t.ModelTarget) || !delegationID(t.Alias) || seen[t.Pool] || aliases[t.Alias] || t.Alias == source.Alias && t.ModelTarget != source.ModelTarget {
			return NativeAuthorization{}, ErrRejected
		}
		seen[t.Pool] = true
		aliases[t.Alias] = true
		if contains(v.claims.AllowedAliases, t.Alias) && g.Access().IsModelAllowed(string(t.Provider), t.Model) {
			auth.Pools = append(auth.Pools, t.Pool)
		}
	}
	return auth, nil
}

func (k DelegationKey) String() string         { return "delegation key (redacted)" }
func (k DelegationKey) GoString() string       { return k.String() }
func (v *VerifiedDelegation) String() string   { return "verified delegation (redacted)" }
func (v *VerifiedDelegation) GoString() string { return v.String() }

func (v *VerifiedDelegation) PolicyCaps() (weight, contextTokens, outputTokens uint32) {
	if v == nil {
		return 0, 0, 0
	}
	return v.claims.Weight, v.claims.MaxContextTokens, v.claims.MaxOutputTokens
}
