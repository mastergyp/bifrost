package managedadmission

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/admission"
	"github.com/valyala/fasthttp"
)

func checkRejection(t *testing.T, response *http.Response, body []byte, original string) {
	t.Helper()
	parts := strings.Split(response.Header.Get(admission.NoDispatchHeader), ".")
	if len(parts) != 3 || parts[0] != "swnd1" {
		t.Fatal("missing original no-dispatch signature")
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	mac := hmac.New(sha256.New, bytes.Repeat([]byte{11}, 32))
	mac.Write([]byte("sw-gateway-no-dispatch-v1\x00"))
	mac.Write(raw)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		t.Fatal("invalid no-dispatch signature")
	}
	var r, claim map[string]any
	json.Unmarshal(raw, &r)
	claimRaw, _ := base64.RawURLEncoding.DecodeString(strings.Split(original, ".")[1])
	json.Unmarshal(claimRaw, &claim)
	for _, name := range []string{"root_id", "call_id", "purpose", "api", "key_id"} {
		if r[name] != claim[name] {
			t.Fatalf("original %s changed", name)
		}
	}
	h := sha256.New()
	h.Write([]byte("sw-gateway-rejection-body-v1\x00"))
	h.Write(body)
	if r["request_hash"] != claim["body_hash"] || r["response_hash"] != hex.EncodeToString(h.Sum(nil)) || r["status"] != float64(response.StatusCode) || r["outcome"] != "not_dispatched" || r["expires_at_ms"].(float64)-r["issued_at_ms"].(float64) != 30000 {
		t.Fatal("unbound response receipt")
	}
}

func TestActualHTTPNoDispatchReceiptRequiresConfirmedOriginalCancellation(t *testing.T) {
	for _, api := range []string{string(admission.Chat), string(admission.Responses)} {
		for _, stream := range []bool{false, true} {
			for _, mode := range []string{"confirmed-v2", "legacy-true", "already-dispatched", "lost-cancel-ack"} {
				t.Run(fmt.Sprintf("%s/stream=%t/%s", api, stream, mode), func(t *testing.T) {
					control, state := ownedPhysicalSocket(t)
					state.cancelVersion = 2
					state.confirmCancellation = true
					if mode == "legacy-true" {
						state.cancelVersion = 0
					}
					if mode == "already-dispatched" {
						state.confirmCancellation = false
					}
					if mode == "lost-cancel-ack" {
						state.cancelStatus = 503
					}
					producer, err := admission.NewProducer(control, deniedSelectedPreparation{fixedPreparation{"home", []string{"owned-engine"}}}, []schemas.ModelProvider{schemas.OpenAI}, 4, bifrost.NewNoOpLogger())
					if err != nil {
						t.Fatal(err)
					}
					f := fixtureWithInner(t, false, true, producer, "home")
					base, client := ownedHTTP(t, f)
					body := map[string]any{"model": "openai/" + model, "stream": stream}
					if api == string(admission.Chat) {
						body["messages"] = []any{map[string]any{"role": "user", "content": "owned"}}
					} else {
						body["input"] = "owned"
					}
					raw, _ := json.Marshal(body)
					token := ownedHTTPToken(api, raw)
					req, _ := http.NewRequest(http.MethodPost, base+admission.GatewayPrefix+api, bytes.NewReader(raw))
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Authorization", "Bearer "+vkValue)
					req.Header.Set(admission.DelegationHeader, token)
					response, err := client.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					result, _ := io.ReadAll(response.Body)
					if response.StatusCode != 402 {
						t.Fatalf("wrong rejection %d", response.StatusCode)
					}
					if mode == "confirmed-v2" {
						checkRejection(t, response, result, token)
					} else if response.Header.Get(admission.NoDispatchHeader) != "" {
						t.Fatal("unconfirmed cancellation received a financial receipt")
					}
					f.assertUntouched(t)
					state.mu.Lock()
					defer state.mu.Unlock()
					if state.data != 0 {
						t.Fatal("rejected request reached data socket")
					}
				})
			}
		}
	}
}

// Started only by the owned Rust frontend probe while its real controller and
// sender are alive. The policy deliberately denies before model I/O, with the
// original core/HTTP factory/governance and controller cancellation all real.
func TestOwnedGatewayRejectionServer(t *testing.T) {
	if os.Getenv("SW_OWNED_GATEWAY_REJECTION_SERVER") != "1" {
		t.Skip("owned double-frontend fixture only")
	}
	control, err := admission.New(os.Getenv("SW_BIFROST_OWNED_SOCKET"))
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	var engines []string
	if json.Unmarshal([]byte(os.Getenv("SW_BIFROST_OWNED_ENGINES")), &engines) != nil || len(engines) == 0 {
		t.Fatal("owned engine inventory required")
	}
	producer, err := admission.NewProducer(control, deniedSelectedPreparation{fixedPreparation{"qwen", engines}}, []schemas.ModelProvider{schemas.OpenAI}, 4, bifrost.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	f := fixtureWithInner(t, false, true, producer, "qwen")
	base, _ := ownedHTTP(t, f, func(ctx *fasthttp.RequestCtx) {
		if bytes.Contains(ctx.PostBody(), []byte("tamper-receipt")) {
			ctx.Response.Header.Set(admission.NoDispatchHeader, "swnd1.forged.invalid")
		}
	})
	fmt.Println("OWNED_BIFROST_REJECTION_READY " + base)
	done := make(chan struct{})
	go func() { _, _ = io.ReadFull(os.Stdin, make([]byte, 1)); close(done) }()
	select {
	case <-done:
	case <-time.After(45 * time.Second):
		t.Fatal("owned frontend did not close its server")
	}
	f.assertUntouched(t)
}
