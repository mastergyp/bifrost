package admission

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"time"
)

const NoDispatchHeader = "X-SW-No-Dispatch-Token"
const noDispatchDomain = "sw-gateway-no-dispatch-v1\x00"
const rejectionBodyDomain = "sw-gateway-rejection-body-v1\x00"

// Only the original private HTTP wrapper signs this bounded response metadata,
// after Producer has obtained a v2 durable no-historical-dispatch confirmation.
// Status, model error text or absence of an active lease are never proof.
type rejectionClaims struct {
	Version      uint32 `json:"version"`
	KeyID        string `json:"key_id"`
	RootID       string `json:"root_id"`
	CallID       string `json:"call_id"`
	Purpose      string `json:"purpose"`
	API          string `json:"api"`
	RequestHash  string `json:"request_hash"`
	ResponseHash string `json:"response_hash"`
	Status       int    `json:"status"`
	Outcome      string `json:"outcome"`
	IssuedAtMS   uint64 `json:"issued_at_ms"`
	ExpiresAtMS  uint64 `json:"expires_at_ms"`
}

func (v *DelegationVerifier) signRejection(proof *VerifiedDelegation, status int, body []byte, now time.Time) string {
	if v == nil || proof == nil || status < 400 || status > 599 || len(body) > 1<<20 || now.UnixMilli() < 0 {
		return ""
	}
	key, ok := v.keys[proof.claims.KeyID]
	if !ok {
		return ""
	}
	h := sha256.New()
	_, _ = h.Write([]byte(rejectionBodyDomain))
	_, _ = h.Write(body)
	n := uint64(now.UnixMilli())
	c := rejectionClaims{Version: 1, KeyID: proof.claims.KeyID, RootID: proof.claims.RootID,
		CallID: proof.claims.CallID, Purpose: proof.claims.Purpose, API: proof.claims.API,
		RequestHash: proof.claims.BodyHash, ResponseHash: hex.EncodeToString(h.Sum(nil)),
		Status: status, Outcome: "not_dispatched", IssuedAtMS: n, ExpiresAtMS: n + 30000}
	raw, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	mac := hmac.New(sha256.New, key.secret)
	_, _ = mac.Write([]byte(noDispatchDomain))
	_, _ = mac.Write(raw)
	return "swnd1." + base64.RawURLEncoding.EncodeToString(raw) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
