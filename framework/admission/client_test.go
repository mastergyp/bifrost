package admission

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	wire "github.com/maximhq/bifrost/core/providers/utils"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/grant"
	"github.com/valyala/fasthttp"
)

func fixtureRequest() Request {
	session := strings.Repeat("b", 64)
	return Request{ID: "11111111-1111-4111-8111-111111111111", Owner: "22222222-2222-4222-8222-222222222222", SubjectHash: strings.Repeat("a", 64), SessionHash: &session, BodyHash: strings.Repeat("c", 64), Alias: "qwen", EligiblePools: []string{"qwen", "glm", "ds"}, HealthyEngines: []string{"q2", "q3", "g0", "d0"}}
}
func fixtureClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	// macOS sockaddr_un cannot hold testing.T's long temporary path.
	dir, err := os.MkdirTemp("/tmp", "sw-adm-")
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "control.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()
	client, err := New(socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); _ = server.Close(); _ = os.RemoveAll(dir) })
	return client
}
func respond(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}
func ready(w http.ResponseWriter) {
	respond(w, map[string]any{"protocol": 1, "state": "ready", "routing_enforcement": "enabled", "absolute_queue_deadline": true})
}
func queued(w http.ResponseWriter, id string, deadline time.Time) {
	respond(w, map[string]any{"protocol": 1, "ticket": map[string]any{"id": id, "deadline_ms": deadline.UnixMilli()}})
}
func cancellation(w http.ResponseWriter) {
	respond(w, map[string]any{"protocol": 1, "never_dispatched": true})
}

func TestAdmissionDisabledFenceCannotReserve(t *testing.T) {
	var mutations atomic.Int32
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/health" {
			mutations.Add(1)
			t.Error("mutation before enforced dispatcher")
			w.WriteHeader(500)
			return
		}
		respond(w, map[string]any{"protocol": 1, "state": "ready", "routing_enforcement": "not_enabled"})
	})
	_, err := client.Acquire(context.Background(), fixtureRequest(), 1, 1, time.Second)
	if !errors.Is(err, ErrNotEnforced) || mutations.Load() != 0 {
		t.Fatalf("activation fence: %v / %d", err, mutations.Load())
	}
}
func TestAdmissionGrantedKeepsOneOwnerAndDeadline(t *testing.T) {
	original := fixtureRequest()
	deadline := time.Now().Add(2 * time.Second)
	var polls atomic.Int32
	var enqueues atomic.Int32
	var cancels atomic.Int32
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/health":
			ready(w)
		case "/v1/enqueue", "/v1/poll":
			var input struct {
				Protocol int     `json:"protocol"`
				Request  Request `json:"request"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			if input.Protocol != 1 || input.Request.ID != original.ID || input.Request.Owner != original.Owner || input.Request.BodyHash != original.BodyHash || *input.Request.SessionHash != *original.SessionHash {
				t.Error("operation or subject changed")
			}
			if r.URL.Path == "/v1/enqueue" {
				enqueues.Add(1)
				queued(w, original.ID, deadline)
				return
			}
			if polls.Add(1) < 3 {
				respond(w, map[string]any{"protocol": 1, "decision": map[string]any{"status": "busy"}})
				return
			}
			respond(w, map[string]any{"protocol": 1, "decision": map[string]any{"status": "granted", "lease": Lease{ID: original.ID, Pool: "ds", Engine: "d0", Foreign: true, State: "prepared"}}})
		case "/v1/cancel":
			var input struct {
				ID    string `json:"id"`
				Owner string `json:"owner"`
			}
			_ = json.NewDecoder(r.Body).Decode(&input)
			if input.ID != original.ID || input.Owner != original.Owner {
				t.Error("cancel changed owner")
			}
			cancels.Add(1)
			cancellation(w)
		default:
			t.Error("unknown control route")
			w.WriteHeader(404)
		}
	})
	ticket, err := client.Acquire(context.Background(), original, 12, 5, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ticket.Lease.Pool != "ds" || ticket.Deadline.UnixMilli() != deadline.UnixMilli() || polls.Load() != 3 || enqueues.Load() != 1 || cancels.Load() != 0 {
		t.Fatalf("wrong handoff: %+v", ticket)
	}
	if cancelled, err := ticket.Cancel(context.Background()); err != nil || !cancelled || cancels.Load() != 1 {
		t.Fatalf("cleanup: %v", err)
	}
}
func TestAdmissionTimeoutDoesNotResetQueueAndCancels(t *testing.T) {
	original := fixtureRequest()
	var enqueues atomic.Int32
	var cancels atomic.Int32
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/health":
			ready(w)
		case "/v1/enqueue":
			enqueues.Add(1)
			queued(w, original.ID, time.Now().Add(80*time.Millisecond))
		case "/v1/poll":
			respond(w, map[string]any{"protocol": 1, "decision": map[string]any{"status": "busy"}})
		case "/v1/cancel":
			cancels.Add(1)
			cancellation(w)
		}
	})
	before := time.Now()
	_, err := client.Acquire(context.Background(), original, 1, 1, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(before) > 500*time.Millisecond || enqueues.Load() != 1 || cancels.Load() != 1 {
		t.Fatalf("deadline reset or leaked: %v / %v", err, time.Since(before))
	}
}
func TestAdmissionCancelledUnknownEnqueueIsReconciled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var cancels atomic.Int32
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/health":
			ready(w)
		case "/v1/enqueue":
			cancel()
			<-r.Context().Done()
		case "/v1/cancel":
			cancels.Add(1)
			cancellation(w)
		}
	})
	_, err := client.Acquire(ctx, fixtureRequest(), 1, 1, time.Second)
	if !errors.Is(err, context.Canceled) || cancels.Load() != 1 {
		t.Fatalf("unknown enqueue was not cancelled: %v / %d", err, cancels.Load())
	}
}
func TestAdmissionRejectsMalformedAndUnownedReplies(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply any
	}{
		{"wrong_protocol", map[string]any{"protocol": 2, "decision": map[string]any{"status": "busy"}}},
		{"wrong_owner_id", map[string]any{"protocol": 1, "decision": map[string]any{"status": "granted", "lease": Lease{ID: "other", Pool: "qwen", Engine: "q2", State: "prepared"}}}},
		{"already_dispatched", map[string]any{"protocol": 1, "decision": map[string]any{"status": "granted", "lease": Lease{ID: fixtureRequest().ID, Pool: "qwen", Engine: "q2", State: "dispatched"}}}},
		{"unapproved_pool", map[string]any{"protocol": 1, "decision": map[string]any{"status": "granted", "lease": Lease{ID: fixtureRequest().ID, Pool: "other", Engine: "q2", State: "prepared"}}}},
		{"unknown_engine", map[string]any{"protocol": 1, "decision": map[string]any{"status": "granted", "lease": Lease{ID: fixtureRequest().ID, Pool: "qwen", Engine: "unknown", State: "prepared"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cancelled atomic.Int32
			client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/health":
					ready(w)
				case "/v1/enqueue":
					queued(w, fixtureRequest().ID, time.Now().Add(time.Second))
				case "/v1/poll":
					respond(w, tc.reply)
				case "/v1/cancel":
					cancelled.Add(1)
					cancellation(w)
				}
			})
			_, err := client.Acquire(context.Background(), fixtureRequest(), 1, 1, 2*time.Second)
			if !errors.Is(err, ErrUnavailable) || cancelled.Load() != 1 {
				t.Fatalf("accepted bad lease: %v", err)
			}
		})
	}
}
func TestAdmissionDoesNotFollowRedirectOrParseUnboundedResponse(t *testing.T) {
	for _, mode := range []string{"redirect", "oversize", "malformed", "forbidden", "busy"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch mode {
				case "redirect":
					w.Header().Set("Location", "http://127.0.0.1:1/forbidden")
					w.WriteHeader(307)
				case "oversize":
					_, _ = w.Write([]byte(strings.Repeat("x", 8193)))
				case "malformed":
					_, _ = w.Write([]byte("{"))
				case "forbidden":
					w.WriteHeader(403)
				case "busy":
					w.WriteHeader(503)
				}
			})
			_, err := client.Acquire(context.Background(), fixtureRequest(), 1, 1, time.Second)
			expected := ErrUnavailable
			if mode == "forbidden" {
				expected = ErrRejected
			}
			if !errors.Is(err, expected) || calls.Load() != 1 {
				t.Fatalf("unsafe response handling: %v / %d", err, calls.Load())
			}
		})
	}
}
func TestAdmissionParallelOwnersRemainIsolated(t *testing.T) {
	var wg sync.WaitGroup
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/health":
			ready(w)
		case "/v1/enqueue":
			var body struct {
				Request Request `json:"request"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			queued(w, body.Request.ID, time.Now().Add(time.Second))
		case "/v1/poll":
			var body struct {
				Request Request `json:"request"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			respond(w, map[string]any{"protocol": 1, "decision": map[string]any{"status": "granted", "lease": Lease{ID: body.Request.ID, Pool: "qwen", Engine: "q2", State: "prepared"}}})
		case "/v1/cancel":
			cancellation(w)
		}
	})
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := fixtureRequest()
			r.ID = fmt.Sprintf("%08x-1111-4111-8111-111111111111", i+1)
			ticket, err := client.Acquire(context.Background(), r, 1, 1, 2*time.Second)
			if err != nil || ticket.Lease.ID != r.ID {
				t.Errorf("parallel request: %v", err)
				return
			}
			_, _ = ticket.Cancel(context.Background())
		}(i)
	}
	wg.Wait()
}
func TestAdmissionInvalidRequestNeverReachesSocket(t *testing.T) {
	client, err := New("/tmp/sw-admission-missing/control.sock")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, r := range []Request{{}, func() Request { r := fixtureRequest(); r.SubjectHash = "raw-user"; return r }(), func() Request { r := fixtureRequest(); r.Owner = "00000000-0000-0000-0000-000000000000"; return r }()} {
		if _, err := client.Acquire(context.Background(), r, 1, 1, time.Second); !errors.Is(err, ErrRejected) {
			t.Fatalf("invalid request: %v", err)
		}
	}
	if _, err = client.Acquire(context.Background(), fixtureRequest(), 1, 1, 61*time.Second); !errors.Is(err, ErrRejected) {
		t.Fatal(err)
	}
	for _, path := range []string{"control.sock", "/tmp/../control.sock", "/tmp/other.sock"} {
		if _, err := New(path); err == nil {
			t.Fatalf("unsafe socket: %s", path)
		}
	}
}

// This opt-in probe uses only the Rust test's disposable PG/schema and socket.
// It proves the real serializer contract without enabling any data plane.
func TestOwnedRustControlProtocol(t *testing.T) {
	socket := os.Getenv("SW_ADMISSION_OWNED_CONTROL_SOCKET")
	if socket == "" {
		t.Skip("requires an explicitly owned Rust control fixture")
	}
	var r Request
	if err := json.Unmarshal([]byte(os.Getenv("SW_ADMISSION_OWNED_REQUEST")), &r); err != nil || !validRequest(r) {
		t.Fatalf("owned request: %v", err)
	}
	client, err := New(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err = client.Acquire(ctx, r, 1, 1, time.Second); !errors.Is(err, ErrNotEnforced) {
		t.Fatalf("real activation fence: %v", err)
	}
	var enqueued struct {
		Protocol int `json:"protocol"`
		Ticket   struct {
			ID         string `json:"id"`
			DeadlineMS int64  `json:"deadline_ms"`
		} `json:"ticket"`
	}
	input := struct {
		Protocol   int     `json:"protocol"`
		Request    Request `json:"request"`
		Cost       int     `json:"cost"`
		Weight     int     `json:"weight"`
		WaitMS     int     `json:"wait_ms"`
		DeadlineMS int64   `json:"deadline_ms"`
	}{1, r, 1, 1000, 1000, time.Now().Add(time.Second).UnixMilli()}
	if err = client.call(ctx, "enqueue", input, &enqueued); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		_, _ = client.cancel(cleanup, r)
	}()
	if enqueued.Protocol != 1 || enqueued.Ticket.ID != r.ID || enqueued.Ticket.DeadlineMS != input.DeadlineMS {
		t.Fatalf("Rust enqueue contract: %+v", enqueued)
	}
	time.Sleep(25 * time.Millisecond)
	if err = client.call(ctx, "enqueue", input, &enqueued); err != nil || enqueued.Ticket.DeadlineMS != input.DeadlineMS {
		t.Fatalf("Rust original deadline replay: %v", err)
	}
	input.DeadlineMS++
	if err = client.call(ctx, "enqueue", input, &enqueued); !errors.Is(err, ErrRejected) {
		t.Fatalf("Rust allowed deadline change on same call ID: %v", err)
	}
	var polled struct {
		Protocol int `json:"protocol"`
		Decision struct {
			Status string `json:"status"`
			Lease  Lease  `json:"lease"`
		} `json:"decision"`
	}
	if err = client.call(ctx, "poll", map[string]any{"protocol": 1, "request": r}, &polled); err != nil {
		t.Fatal(err)
	}
	if polled.Protocol != 1 || polled.Decision.Status != "granted" || polled.Decision.Lease.ID != r.ID || polled.Decision.Lease.State != "prepared" || !contains(r.EligiblePools, polled.Decision.Lease.Pool) || !contains(r.HealthyEngines, polled.Decision.Lease.Engine) {
		t.Fatalf("Rust lease contract: %+v", polled)
	}
	if cancelled, err := client.cancel(ctx, r); err != nil || !cancelled {
		t.Fatalf("Rust never-dispatched cancellation: %v / %v", cancelled, err)
	}
	t.Log("owned_rust_control_protocol_passed=true; inference_dispatched=false")
}

func TestAdmissionCancelNeverClaimsDispatchedWorkWasReleased(t *testing.T) {
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/cancel" {
			t.Fatal("unexpected control route")
		}
		respond(w, map[string]any{"protocol": 1, "never_dispatched": false})
	})
	if cancelled, err := client.cancel(context.Background(), fixtureRequest()); err != nil || cancelled {
		t.Fatalf("claimed live work was released: %v / %v", cancelled, err)
	}
}

// No public route reaches this client until the authenticated last pre-LLM hook
// is registered. These are actual Unix HTTP requests, not a fake RoundTripper.
func ownedTicket(client *Client, api API, body []byte) *Ticket {
	r := fixtureRequest()
	r.BodyHash, _ = RequestDigest(api, body)
	return &Ticket{client: client, request: r, Lease: Lease{ID: r.ID, Pool: "qwen", Engine: "q2", State: "prepared"}}
}
func TestInferenceDigestMatchesRustAndRejectsOtherAPI(t *testing.T) {
	for api, want := range map[API]string{Chat: "b147b409032a684ef3451e179d9692cbbf5497a9a97e75f7c64b5403c083906c", Responses: "64b30db07a8eb6e7a14d1fc05458fdf3ca5c124e512c80ca81b6816373e00395"} {
		got, err := RequestDigest(api, []byte("{}"))
		if err != nil || got != want {
			t.Fatalf("Rust digest vector for %s: %s / %v", api, got, err)
		}
	}
	var calls atomic.Int32
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); respond(w, map[string]any{"ok": true}) })
	ticket := ownedTicket(client, Chat, []byte("{}"))
	for _, attempt := range []struct {
		api  API
		body []byte
	}{{Responses, []byte("{}")}, {Chat, []byte("{} ")}, {API("/v1/chat/completions?target=ds"), []byte("{}")}} {
		if _, err := ticket.Forward(context.Background(), attempt.api, attempt.body); !errors.Is(err, ErrRejected) {
			t.Fatalf("mismatched dispatch accepted: %v", err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("I/O before digest/API validation")
	}
	resp, err := ticket.Forward(context.Background(), Chat, []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if _, err = ticket.Forward(context.Background(), Chat, []byte("{}")); !errors.Is(err, ErrRejected) || calls.Load() != 1 {
		t.Fatalf("replayed inference: %v / %d", err, calls.Load())
	}
}
func TestInferenceLargeBodyUsesPrivateHeadersAndKeepsControlResponsive(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }) }
	defer finish()
	started := make(chan struct{}, 12)
	body := []byte(`{"model":"logical","messages":[{"role":"user","content":"` + strings.Repeat("x", 65536) + `"}],"stream":true}`)
	var calls atomic.Int32
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/health" {
			ready(w)
			return
		}
		if r.URL.Path != "/inference/v1/chat/completions" || r.Header.Get("Authorization") != "" || len(r.Header.Values("X-SW-Admission-Owner")) != 1 || r.Header.Get("X-SW-Admission-Alias") != "qwen" {
			t.Error("wrong path or leaked credentials")
		}
		actual, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, actual) {
			t.Error("forwarded bytes changed")
		}
		calls.Add(1)
		started <- struct{}{}
		<-release
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-SW-Resolved-Engine", "q2")
		w.Header().Set("X-SW-Admission-Owner", fixtureRequest().Owner)
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	})
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := ownedTicket(client, Chat, body).Forward(context.Background(), Chat, body)
			if err != nil {
				t.Error(err)
				return
			}
			defer response.Body.Close()
			data, err := io.ReadAll(response.Body)
			if err != nil || string(data) != "data: [DONE]\n\n" || response.Header.Get("X-SW-Resolved-Engine") != "q2" || response.Header.Get("X-SW-Admission-Owner") != "" {
				t.Errorf("wrong response metadata/bytes: %v", err)
			}
		}()
	}
	for range 12 {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("data calls consumed control pool or stalled")
		}
	}
	var health map[string]any
	if err := client.call(context.Background(), "health", nil, &health); err != nil {
		t.Fatalf("health blocked by waiting inferences: %v", err)
	}
	// Finish handlers before fixture cleanup; deferred release also covers failure.
	finish()
	wg.Wait()
	if calls.Load() != 12 {
		t.Fatal("missing inference")
	}
}
func TestInferenceDoesNotReplayAmbiguousSendOrFollowRedirect(t *testing.T) {
	for _, kind := range []string{"disconnect", "redirect", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int32
			client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch kind {
				case "disconnect":
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
				case "redirect":
					w.Header().Set("Location", "http://external.invalid/replay")
					w.WriteHeader(307)
				case "oversize":
					w.Header().Set("Content-Length", fmt.Sprint(maxResponseBytes+1))
					w.WriteHeader(200)
				}
			})
			ticket := ownedTicket(client, Chat, []byte("{}"))
			if resp, err := ticket.Forward(context.Background(), Chat, []byte("{}")); err == nil {
				resp.Body.Close()
				t.Fatal("ambiguous/redirect/oversized response accepted")
			}
			if _, err := ticket.Forward(context.Background(), Chat, []byte("{}")); !errors.Is(err, ErrRejected) || calls.Load() != 1 {
				t.Fatalf("ambiguous send replayed: %v / %d", err, calls.Load())
			}
		})
	}
}
func TestInferenceBoundedChunkedBody(t *testing.T) {
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		_, _ = io.CopyN(w, strings.NewReader(strings.Repeat("x", maxResponseBytes+1)), maxResponseBytes+1)
	})
	response, err := ownedTicket(client, Chat, []byte("{}")).Forward(context.Background(), Chat, []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if _, err = io.Copy(io.Discard, response.Body); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unbounded chunked response: %v", err)
	}
}

func TestOwnedRustInferenceProtocol(t *testing.T) {
	socket := os.Getenv("SW_ADMISSION_OWNED_CONTROL_SOCKET")
	if socket == "" {
		t.Skip("requires an explicitly owned Rust inference fixture")
	}
	var request Request
	if err := json.Unmarshal([]byte(os.Getenv("SW_ADMISSION_OWNED_REQUEST")), &request); err != nil || !validRequest(request) {
		t.Fatal("bad owned request")
	}
	body := []byte(os.Getenv("SW_ADMISSION_OWNED_BODY"))
	if len(body) == 0 || len(body) > maxInferenceBytes {
		t.Fatal("bad owned body")
	}
	digest, err := RequestDigest(Chat, body)
	if err != nil || digest != request.BodyHash {
		t.Fatal("Go/Rust API/body digest disagrees")
	}
	client, err := New(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The owned Rust fixture already reserves the exact body. Bind its same ID
	// to an absolute queue deadline before using the real Unix inference path;
	// this private probe does not enable the production-wide activation fence.
	deadline := time.Now().Add(2 * time.Second).UnixMilli()
	var queuedReply struct {
		Ticket struct {
			ID         string `json:"id"`
			DeadlineMS int64  `json:"deadline_ms"`
		} `json:"ticket"`
	}
	if err = client.call(ctx, "enqueue", map[string]any{"protocol": 1, "request": request, "cost": 1, "weight": 1000, "wait_ms": 2000, "deadline_ms": deadline}, &queuedReply); err != nil || queuedReply.Ticket.ID != request.ID || queuedReply.Ticket.DeadlineMS != deadline {
		t.Fatalf("owned inference original deadline: %v", err)
	}
	var prepared struct {
		Decision struct {
			Status string `json:"status"`
			Lease  Lease  `json:"lease"`
		} `json:"decision"`
	}
	if err = client.call(ctx, "poll", map[string]any{"protocol": 1, "request": request}, &prepared); err != nil || prepared.Decision.Status != "granted" || prepared.Decision.Lease.State != "prepared" {
		t.Fatalf("owned inference queue grant: %v", err)
	}
	ticket := &Ticket{client: client, request: request, Lease: prepared.Decision.Lease, Deadline: time.UnixMilli(deadline)}
	if _, err = ticket.Forward(ctx, Responses, body); !errors.Is(err, ErrRejected) {
		t.Fatal("Chat ticket accepted for Responses")
	}
	response, err := ticket.Forward(ctx, Chat, body)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	result, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != 200 || !bytes.Contains(result, []byte("owned fixture complete")) || response.Header.Get("X-SW-Requested-Model") != "qwen" {
		t.Fatalf("Rust managed inference response: %d / %v", response.StatusCode, err)
	}
	if _, err = ticket.Forward(ctx, Chat, body); !errors.Is(err, ErrRejected) {
		t.Fatal("Go replayed ticket")
	}
	if confirmed, err := ticket.ConfirmNeverDispatched(ctx); err != nil || confirmed {
		t.Fatalf("a completed actual Rust dispatch was certified as no-I/O: %t / %v", confirmed, err)
	}
	t.Log("owned_rust_inference_protocol_passed=true; synthetic_http_dispatch=true; absolute_deadline_preserved=true; production_enforcement=false")
}

func TestNoDispatchConfirmationRequiresHistoricalEvidenceVersion(t *testing.T) {
	for _, test := range []struct {
		name      string
		body      string
		want      bool
		wantError bool
	}{
		{"prepared-cancelled", `{"protocol":1,"never_dispatched":true,"no_dispatch_evidence_version":2}`, true, false},
		{"already-executed", `{"protocol":1,"never_dispatched":false,"no_dispatch_evidence_version":2}`, false, false},
		{"legacy-true", `{"protocol":1,"never_dispatched":true}`, false, true},
		{"legacy-false", `{"protocol":1,"never_dispatched":false}`, false, true},
		{"missing-state", `{"protocol":1,"no_dispatch_evidence_version":2}`, false, true},
		{"null-state", `{"protocol":1,"never_dispatched":null,"no_dispatch_evidence_version":2}`, false, true},
		{"wrong-version", `{"protocol":1,"never_dispatched":true,"no_dispatch_evidence_version":3}`, false, true},
		{"wrong-protocol", `{"protocol":2,"never_dispatched":true,"no_dispatch_evidence_version":2}`, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := fixtureRequest()
			client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/cancel" || r.Method != http.MethodPost {
					t.Error("wrong confirmation endpoint")
				}
				var input struct {
					ID    string `json:"id"`
					Owner string `json:"owner"`
				}
				if json.NewDecoder(r.Body).Decode(&input) != nil || input.ID != original.ID || input.Owner != original.Owner {
					t.Error("confirmation changed original ownership")
				}
				_, _ = w.Write([]byte(test.body))
			})
			ticket := &Ticket{client: client, request: original}
			confirmed, err := ticket.ConfirmNeverDispatched(context.Background())
			if confirmed != test.want || (err != nil) != test.wantError {
				t.Fatalf("unsafe confirmation: %t / %v", confirmed, err)
			}
		})
	}
}

func nativeGrantContext(vk, user string, providers []schemas.ProviderPermit) *schemas.BifrostContext {
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	g := grant.New()
	g.SetIdentity(grant.NewIdentity(grant.NewCredential(grant.CredentialVirtualKey, "synthetic-value-never-read"), &schemas.UserRef{ID: user}, &schemas.EntityRef{ID: vk}, nil, nil, nil, nil))
	permit := grant.NewPermit(grant.PermitVirtualKey, vk, "synthetic", true, false, providers, nil)
	g.SetAccess(grant.NewAccess([]schemas.Permit{permit}, nil, "", nil))
	g.SetLimits(grant.NewLimits(nil, nil))
	ctx.SetGrant(g)
	return ctx
}
func nativeTargets() []ModelTarget {
	return []ModelTarget{{Pool: "qwen", Provider: "gpu0", Model: "qwen"}, {Pool: "glm", Provider: "gpu2", Model: "glm"}, {Pool: "ds", Provider: "gpu1", Model: "ds"}}
}
func nativePermits(all bool) []schemas.ProviderPermit {
	result := []schemas.ProviderPermit{{Provider: "gpu0", AllowedModels: schemas.WhiteList{"qwen"}, KeyIDs: schemas.WhiteList{"*"}}}
	if all {
		result = append(result, schemas.ProviderPermit{Provider: "gpu2", AllowedModels: schemas.WhiteList{"glm"}, KeyIDs: schemas.WhiteList{"*"}}, schemas.ProviderPermit{Provider: "gpu1", AllowedModels: schemas.WhiteList{"*"}, KeyIDs: schemas.WhiteList{"*"}})
	}
	return result
}
func TestNativeModelAuthorizationPreservesOriginalScopesAndLimits(t *testing.T) {
	authorizer, err := NewNativeAuthorizer([]byte(strings.Repeat("s", 32)), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, all := range []bool{false, true} {
		ctx := nativeGrantContext("original-vk", "original-user", nativePermits(all))
		before := ctx.Grant().Limits()
		auth, err := authorizer.Authorize(ctx, nativeTargets()[0], nativeTargets())
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"qwen"}
		if all {
			want = []string{"qwen", "glm", "ds"}
		}
		if !reflect.DeepEqual(auth.Pools, want) || !validDigest(auth.SubjectHash) || auth.OriginalLimits != before || ctx.Grant().Limits() != before {
			t.Fatalf("native permissions/limits changed: %+v / %v", auth, err)
		}
	}
	providers := nativePermits(true)
	providers[2].BlacklistedModels = schemas.BlackList{"ds"}
	ctx := nativeGrantContext("vk", "user", providers)
	auth, err := authorizer.Authorize(ctx, nativeTargets()[0], nativeTargets())
	if err != nil || !reflect.DeepEqual(auth.Pools, []string{"qwen", "glm"}) {
		t.Fatalf("blacklisted model became eligible: %+v / %v", auth, err)
	}
	// A broad base composed with a Qwen-only scoping permit must still stay Qwen-only.
	scope := grant.NewPermit(grant.PermitAccessProfile, "scope", "limited", true, false, nativePermits(false), nil)
	broad := grant.NewPermit(grant.PermitVirtualKey, "vk", "broad", true, false, nativePermits(true), nil)
	ctx.Grant().SetAccess(grant.NewAccess([]schemas.Permit{broad}, scope, grant.Intersect, nil))
	auth, err = authorizer.Authorize(ctx, nativeTargets()[0], nativeTargets())
	if err != nil || !reflect.DeepEqual(auth.Pools, []string{"qwen"}) {
		t.Fatal("scoping restriction was broadened")
	}
}
func TestNativeModelAuthorizationRejectsUnsettledAndDelegatedIdentities(t *testing.T) {
	secret := []byte(strings.Repeat("s", 32))
	delegated := []string{"gateway-transit"}
	authorizer, err := NewNativeAuthorizer(secret, delegated)
	if err != nil {
		t.Fatal(err)
	}
	delegated[0] = "changed-by-caller"
	secret[0] = 'x' // construction owns both inputs
	ctx := nativeGrantContext("gateway-transit", "gateway-service", nativePermits(true))
	if _, err = authorizer.Authorize(ctx, nativeTargets()[0], nativeTargets()); !errors.Is(err, ErrDelegationRequired) {
		t.Fatalf("service VK impersonated its original Gateway user: %v", err)
	}
	empty := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	if _, err = authorizer.Authorize(empty, nativeTargets()[0], nativeTargets()); !errors.Is(err, ErrGrantUnresolved) {
		t.Fatal("missing grant accepted")
	}
	g := grant.New()
	empty.SetGrant(g)
	if _, err = authorizer.Authorize(empty, nativeTargets()[0], nativeTargets()); !errors.Is(err, ErrGrantUnresolved) || g.Access() != nil {
		t.Fatal("unsettled access was changed or accepted")
	}
	ctx = nativeGrantContext("native", "user", nativePermits(false))
	if _, err = authorizer.Authorize(ctx, nativeTargets()[2], nativeTargets()); !errors.Is(err, ErrSourceDenied) {
		t.Fatal("unauthorized source model accepted")
	}
}
func TestNativePrincipalAndExplicitSessionSurviveKeyValueRotationButIsolateUsers(t *testing.T) {
	authorizer, _ := NewNativeAuthorizer([]byte(strings.Repeat("s", 32)), nil)
	ctx := nativeGrantContext("stable-key-row", "user-a", nativePermits(true))
	first, err := authorizer.Authorize(ctx, nativeTargets()[0], nativeTargets())
	if err != nil {
		t.Fatal(err)
	}
	ctx.Grant().SetIdentity(grant.NewIdentity(grant.NewCredential(grant.CredentialVirtualKey, "rotated-value"), &schemas.UserRef{ID: "user-a"}, &schemas.EntityRef{ID: "stable-key-row"}, nil, nil, nil, nil))
	rotated, err := authorizer.Authorize(ctx, nativeTargets()[0], nativeTargets())
	if err != nil || first.SubjectHash != rotated.SubjectHash {
		t.Fatal("credential value changed the native principal")
	}
	other, err := authorizer.Authorize(nativeGrantContext("stable-key-row", "user-b", nativePermits(true)), nativeTargets()[0], nativeTargets())
	if err != nil || first.SubjectHash == other.SubjectHash {
		t.Fatal("different employees share a native subject")
	}
	if session, err := authorizer.SessionHash(first.SubjectHash, nil); err != nil || session != nil {
		t.Fatal("missing conversation was replaced with a user hash")
	}
	sessionID := "explicit-conversation"
	a, _ := authorizer.SessionHash(first.SubjectHash, &sessionID)
	b, _ := authorizer.SessionHash(other.SubjectHash, &sessionID)
	if a == nil || b == nil || *a == *b {
		t.Fatal("conversation leaked across subjects")
	}
}

func TestUninitializedNativeAuthorizerCannotMintIdentityOrSession(t *testing.T) {
	ctx := nativeGrantContext("native", "user", nativePermits(true))
	for _, a := range []*NativeAuthorizer{nil, {}} {
		if _, err := a.Authorize(ctx, nativeTargets()[0], nativeTargets()); !errors.Is(err, ErrRejected) {
			t.Fatal("uninitialized authorizer minted a predictable subject")
		}
		if _, err := a.SessionHash(strings.Repeat("a", 64), new("conversation")); !errors.Is(err, ErrRejected) {
			t.Fatal("uninitialized authorizer minted a predictable session")
		}
	}
}

func delegationFixture(t *testing.T) (*DelegationVerifier, delegationClaims, []byte, time.Time) {
	t.Helper()
	now := time.UnixMilli(1793580000000)
	body := []byte(`{"model":"qwen","messages":[{"role":"user","content":"owned fixture"}]}`)
	hash, _ := RequestDigest(Chat, body)
	v, err := NewDelegationVerifier([]DelegationKey{{ID: "owned-key-1", Secret: bytes.Repeat([]byte{11}, 32), GatewayVirtualKeyIDs: []string{"owned-gateway-transit"}}})
	if err != nil {
		t.Fatal(err)
	}
	c := delegationClaims{Version: 1, KeyID: "owned-key-1", RootID: "11111111-1111-4111-8111-111111111111", CallID: "22222222-2222-4222-8222-222222222222", SubjectHash: strings.Repeat("a", 64), SourceAlias: "qwen", AllowedAliases: []string{"qwen"}, Purpose: "main", API: string(Chat), BodyHash: hash, IssuedAtMS: uint64(now.UnixMilli()), ExpiresAtMS: uint64(now.UnixMilli() + 60000), QueueDeadlineMS: uint64(now.UnixMilli() + 45000), Weight: 1, MaxContextTokens: 32768, MaxOutputTokens: 4096, OriginalControls: json.RawMessage(`{"reasoning_effort":"low","temperature":0}`)}
	return v, c, body, now
}
func delegationFixtureToken(t *testing.T, c delegationClaims, replace func([]byte) []byte) string {
	t.Helper()
	payload, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if replace != nil {
		payload = replace(payload)
	}
	h := hmac.New(sha256.New, bytes.Repeat([]byte{11}, 32))
	h.Write([]byte(delegationDomain))
	h.Write(payload)
	return "swgd1." + base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}
func TestGatewayDelegationRequiresExactRequestValidTimeAndResolvedTransitGrant(t *testing.T) {
	v, c, body, now := delegationFixture(t)
	token := delegationFixtureToken(t, c, nil)
	proof, err := v.Verify([]string{token}, "POST", Chat, body, now)
	if err != nil {
		t.Fatal(err)
	}
	weight, contextTokens, outputTokens := proof.PolicyCaps()
	if weight != 1 || contextTokens != 32768 || outputTokens != 4096 {
		t.Fatal("original policy caps changed")
	}
	ctx := nativeGrantContext("owned-gateway-transit", "service", nativePermits(true))
	before := ctx.Grant().Limits()
	targets := []DelegationTarget{{"qwen", nativeTargets()[0]}, {"glm", nativeTargets()[1]}, {"ds", nativeTargets()[2]}}
	auth, err := proof.Authorize(ctx, targets[0], targets, now)
	if err != nil || !reflect.DeepEqual(auth.Pools, []string{"qwen"}) || auth.SubjectHash != c.SubjectHash || auth.OriginalLimits != before || ctx.Grant().Limits() != before {
		t.Fatalf("delegated original scope/limits changed: %v", err)
	}
	if _, err = proof.Authorize(nativeGrantContext("employee-key", "employee", nativePermits(true)), targets[0], targets, now); !errors.Is(err, ErrDelegationRequired) {
		t.Fatal("native employee key could use Gateway attestation")
	}
	if _, err = proof.Authorize(ctx, targets[0], targets, now.Add(time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatal("proof survived waiting past its original deadline")
	}
	for _, tc := range []struct {
		method string
		api    API
		body   []byte
		values []string
		at     time.Time
	}{{"GET", Chat, body, []string{token}, now}, {"POST", Responses, body, []string{token}, now}, {"POST", Chat, []byte(`{}`), []string{token}, now}, {"POST", Chat, body, []string{token, token}, now}, {"POST", Chat, body, []string{token}, now.Add(time.Minute)}} {
		if _, err = v.Verify(tc.values, tc.method, tc.api, tc.body, tc.at); err == nil {
			t.Fatal("altered/expired request authorized")
		}
	}
	controls := proof.OriginalControls()
	controls[0] = '!'
	if !json.Valid(proof.OriginalControls()) {
		t.Fatal("caller mutated verified control snapshot")
	}
}
func TestGatewayDelegationRejectsSignedParserAmbiguityAndExcessScope(t *testing.T) {
	v, c, body, now := delegationFixture(t)
	for _, change := range []func(*delegationClaims){func(c *delegationClaims) { c.Purpose = "image_observation"; c.AllowedAliases = []string{"glm", "qwen"} }, func(c *delegationClaims) { c.AllowedAliases = []string{"qwen", "qwen"} }, func(c *delegationClaims) { c.QueueDeadlineMS = c.IssuedAtMS + 60001 }, func(c *delegationClaims) { c.OriginalControls = json.RawMessage(`{"provider":"evil"}`) }, func(c *delegationClaims) { c.CallID = "caller-request-id" }, func(c *delegationClaims) { c.Weight = 0 }, func(c *delegationClaims) { c.Weight = 1001 }, func(c *delegationClaims) { c.MaxOutputTokens = 0 }, func(c *delegationClaims) { c.MaxOutputTokens = c.MaxContextTokens + 1 }} {
		copy := c
		change(&copy)
		if _, err := v.Verify([]string{delegationFixtureToken(t, copy, nil)}, "POST", Chat, body, now); err == nil {
			t.Fatal("invalid signed claims accepted")
		}
	}
	for _, replace := range []func([]byte) []byte{func(b []byte) []byte { return append([]byte(`{"key_id":"duplicate",`), b[1:]...) }, func(b []byte) []byte { return append(b, []byte(` {}`)...) }, func(b []byte) []byte { return append([]byte(`{"unknown":1,`), b[1:]...) }} {
		if _, err := v.Verify([]string{delegationFixtureToken(t, c, replace)}, "POST", Chat, body, now); err == nil {
			t.Fatal("ambiguous signed JSON accepted")
		}
	}
	token := delegationFixtureToken(t, c, nil)
	for _, bad := range []string{token + "=", " " + token, strings.Replace(token, "swgd1.", "swgd1.\n", 1)} {
		if _, err := v.Verify([]string{bad}, "POST", Chat, body, now); err == nil {
			t.Fatal("noncanonical envelope accepted")
		}
	}
}
func TestOwnedRustGatewayDelegationProtocol(t *testing.T) {
	path := os.Getenv("SW_OWNED_RUST_DELEGATION_VECTOR")
	stdin := os.Getenv("SW_OWNED_GATEWAY_DELEGATION_STDIN") == "1"
	if path == "" && !stdin {
		t.Skip("requires an explicitly owned Rust-signed fixture")
	}
	var raw []byte
	var err error
	if stdin {
		raw, err = io.ReadAll(io.LimitReader(os.Stdin, (8<<20)+1))
	} else {
		raw, err = os.ReadFile(path)
	}
	if err != nil || len(raw) > 8<<20 {
		t.Fatal("owned input unavailable")
	}
	var input struct {
		Token           string   `json:"token"`
		Body            string   `json:"body"`
		NowMS           int64    `json:"now_ms"`
		ExpectedPools   []string `json:"expected_pools"`
		ExpectedSubject string   `json:"expected_subject"`
	}
	if json.Unmarshal(raw, &input) != nil {
		t.Fatal("owned fixture invalid")
	}
	at := time.UnixMilli(input.NowMS)
	if stdin {
		at = time.Now()
	}
	v, _, _, _ := delegationFixture(t)
	proof, err := v.Verify([]string{input.Token}, "POST", Chat, []byte(input.Body), at)
	if err != nil || !validUUID(proof.CallID()) {
		t.Fatal("actual Rust signature/body did not verify")
	}
	ctx := nativeGrantContext("owned-gateway-transit", "service", nativePermits(true))
	targets := []DelegationTarget{{proof.claims.SourceAlias, nativeTargets()[0]}, {"glm", nativeTargets()[1]}, {"ds", nativeTargets()[2]}}
	auth, err := proof.Authorize(ctx, targets[0], targets, at)
	expected := input.ExpectedPools
	if expected == nil {
		expected = []string{"qwen", "glm", "ds"}
	}
	if err != nil || !reflect.DeepEqual(auth.Pools, expected) || input.ExpectedSubject != "" && auth.SubjectHash != input.ExpectedSubject || auth.OriginalLimits != ctx.Grant().Limits() {
		t.Fatal("Rust-signed original scope did not intersect real Grant")
	}
	if !schemas.IsSensitiveHeader(DelegationHeader) {
		t.Fatal("delegation token would leak through trace headers")
	}
}

type runtimeProbe struct {
	calls     int
	principal *Principal
}

func (*runtimeProbe) Owns(p schemas.ModelProvider) bool {
	return p == "gpu0" || p == "gpu1" || p == "gpu2"
}
func (p *runtimeProbe) Close() {}
func (p *runtimeProbe) Handle(ctx *schemas.BifrostContext, _ *schemas.BifrostRequest) *schemas.LLMPluginShortCircuit {
	p.calls++
	p.principal = AdmittedPrincipal(ctx)
	return &schemas.LLMPluginShortCircuit{Response: &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{ID: "owned"}}}
}
func runtimeFixture(t *testing.T) (*Runtime, *runtimeProbe) {
	t.Helper()
	verifier, _, _, _ := delegationFixture(t)
	authorizer, _ := NewNativeAuthorizer([]byte(strings.Repeat("s", 32)), nil)
	probe := &runtimeProbe{}
	targets := nativeTargets()
	runtime, err := NewRuntime(probe, authorizer, verifier, []RuntimeModel{{"qwen", targets[0]}, {"glm", targets[1]}, {"ds", targets[2]}}, []string{"owned-gateway-transit"})
	if err != nil {
		t.Fatal(err)
	}
	return runtime, probe
}
func runtimeToken(t *testing.T, api API, body []byte) string {
	_, c, _, _ := delegationFixture(t)
	n := uint64(time.Now().UnixMilli())
	c.IssuedAtMS = n
	c.ExpiresAtMS = n + 60000
	c.QueueDeadlineMS = n + 45000
	c.API = string(api)
	c.BodyHash, _ = RequestDigest(api, body)
	return delegationFixtureToken(t, c, nil)
}
func TestGatewayRuntimeHTTPRejectsMissingAlteredOrMisroutedProofBeforeDispatch(t *testing.T) {
	body := []byte(`{"model":"gpu0/qwen","messages":[]}`)
	for _, tc := range []struct {
		name, method, path         string
		api                        API
		body                       []byte
		token, duplicate, override bool
		status                     int
	}{
		{"chat missing", "POST", GatewayPrefix + string(Chat), Chat, body, false, false, false, 401},
		{"responses missing", "POST", GatewayPrefix + string(Responses), Responses, body, false, false, false, 401},
		{"altered body", "POST", GatewayPrefix + string(Chat), Chat, []byte(`{}`), true, false, false, 401},
		{"wrong API", "POST", GatewayPrefix + string(Responses), Chat, body, true, false, false, 401},
		{"duplicate", "POST", GatewayPrefix + string(Chat), Chat, body, true, true, false, 401},
		{"native channel", "POST", "/openai" + string(Chat), Chat, body, true, false, false, 401},
		{"method", "GET", GatewayPrefix + string(Chat), Chat, body, true, false, false, 405},
		{"unknown", "POST", GatewayPrefix + "/other", Chat, body, true, false, false, 404},
		{"routing override", "POST", GatewayPrefix + string(Chat), Chat, body, true, false, true, 400},
		{"query override", "POST", GatewayPrefix + string(Chat) + "?model=other", Chat, body, true, false, false, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime, probe := runtimeFixture(t)
			ctx := &fasthttp.RequestCtx{}
			ctx.Request.SetRequestURI(tc.path)
			ctx.Request.Header.SetMethod(tc.method)
			ctx.Request.SetBody(tc.body)
			if tc.token {
				token := runtimeToken(t, tc.api, body)
				ctx.Request.Header.Set(DelegationHeader, token)
				if tc.duplicate {
					ctx.Request.Header.Add(DelegationHeader, token)
				}
			}
			if tc.override {
				ctx.Request.Header.Set("x-bf-compat", "true")
			}
			next := 0
			runtime.WrapHTTP(func(*fasthttp.RequestCtx) { next++ })(ctx)
			if ctx.Response.StatusCode() != tc.status || next != 0 || probe.calls != 0 {
				t.Fatalf("guard status=%d next=%d managed=%d", ctx.Response.StatusCode(), next, probe.calls)
			}
		})
	}
}
func TestGatewayRuntimePreservesOriginalGrantAndSharedLegacyHomeAccess(t *testing.T) {
	for _, gateway := range []bool{false, true} {
		t.Run(fmt.Sprint(gateway), func(t *testing.T) {
			runtime, probe := runtimeFixture(t)
			ctx := &fasthttp.RequestCtx{}
			path := "/openai" + string(Chat)
			body := []byte(`{"model":"gpu0/qwen"}`)
			if gateway {
				path = GatewayPrefix + string(Chat)
				ctx.Request.Header.Set(DelegationHeader, runtimeToken(t, Chat, body))
			}
			ctx.Request.SetRequestURI(path)
			ctx.Request.Header.SetMethod("POST")
			ctx.Request.SetBody(body)
			ctx.Request.Header.Set("x-session-id", "explicit-owned-session")
			runtime.WrapHTTP(func(httpCtx *fasthttp.RequestCtx) {
				bf := nativeGrantContext("owned-gateway-transit", "service", nativePermits(true))
				before := bf.Grant().Limits()
				httpCtx.VisitUserValuesAll(func(k, v any) { bf.SetValue(k, v) })
				if !runtime.RequiresManaged(bf) {
					t.Fatal("trusted ingress provenance lost")
				}
				result := runtime.Handle(bf, &schemas.BifrostRequest{RequestType: schemas.ChatCompletionRequest, ChatRequest: &schemas.BifrostChatRequest{Provider: "gpu0", Model: "qwen"}})
				if result.Error != nil || probe.calls != 1 || probe.principal == nil {
					t.Fatal("authorized source failed")
				}
				principal := probe.principal
				auth := principal.Authorization()
				if !reflect.DeepEqual(auth.Pools, []string{"qwen"}) || auth.OriginalLimits != before || bf.Grant().Limits() != before {
					t.Fatal("scope, shared home access or original limits changed")
				}
				if gateway {
					if principal.Delegation() == nil || principal.SessionHash() != nil {
						t.Fatal("unsigned session replaced signed absent session")
					}
				} else if principal.SessionHash() == nil {
					t.Fatal("explicit native conversation lost")
				}
				auth.Pools[0] = "tampered"
				if principal.Authorization().Pools[0] != "qwen" {
					t.Fatal("principal scope is mutable")
				}
			})(ctx)
			if probe.calls != 1 {
				t.Fatal("native or delegated access dropped")
			}
			if string(ctx.Path()) != path {
				t.Fatal("transport path was not restored")
			}
		})
	}
}
func TestGatewayRuntimeRequiresSignedSourceAndResolvedTrustedVK(t *testing.T) {
	for _, tc := range []struct {
		vk       string
		provider schemas.ModelProvider
		model    string
	}{
		{"employee-key", "gpu0", "qwen"}, {"owned-gateway-transit", "gpu1", "ds"},
	} {
		runtime, probe := runtimeFixture(t)
		httpCtx := &fasthttp.RequestCtx{}
		body := []byte(`{"model":"gpu0/qwen"}`)
		httpCtx.Request.SetRequestURI(GatewayPrefix + string(Chat))
		httpCtx.Request.Header.SetMethod("POST")
		httpCtx.Request.SetBody(body)
		httpCtx.Request.Header.Set(DelegationHeader, runtimeToken(t, Chat, body))
		runtime.WrapHTTP(func(c *fasthttp.RequestCtx) {
			bf := nativeGrantContext(tc.vk, "service", nativePermits(true))
			c.VisitUserValuesAll(func(k, v any) { bf.SetValue(k, v) })
			result := runtime.Handle(bf, &schemas.BifrostRequest{RequestType: schemas.ChatCompletionRequest, ChatRequest: &schemas.BifrostChatRequest{Provider: tc.provider, Model: tc.model}})
			if result.Error == nil || probe.calls != 0 {
				t.Fatal("credential/source override reached inner producer")
			}
		})(httpCtx)
	}
	var disabled *Runtime
	next := 0
	c := &fasthttp.RequestCtx{}
	c.Request.SetRequestURI("/openai/v1/models")
	disabled.WrapHTTP(func(*fasthttp.RequestCtx) { next++ })(c)
	if next != 1 {
		t.Fatal("disabled feature changed normal endpoints")
	}
	c.Request.SetRequestURI(GatewayPrefix + string(Chat))
	c.Request.Header.SetMethod("POST")
	disabled.WrapHTTP(func(*fasthttp.RequestCtx) { next++ })(c)
	if c.Response.StatusCode() != 401 || next != 1 {
		t.Fatal("disabled channel escaped")
	}
}

func TestManagedRuntimeRejectsAmbiguousDeploymentInputsAndRedactsPrivateHandles(t *testing.T) {
	r, p := runtimeFixture(t)
	for _, models := range [][]RuntimeModel{nil, append(append([]RuntimeModel(nil), r.models...), r.models[0]), {{Alias: "bad/alias", ModelTarget: nativeTargets()[0]}}, {{Alias: "other", ModelTarget: ModelTarget{Pool: "qwen", Provider: "unowned", Model: "qwen"}}}} {
		if _, err := NewRuntime(p, r.native, r.verifier, models, nil); err == nil {
			t.Fatal("ambiguous/unowned registry accepted")
		}
	}
	if _, err := NewRuntime(nil, r.native, r.verifier, r.models, nil); err == nil {
		t.Fatal("missing producer accepted")
	}
	if _, err := NewRuntime(p, r.native, r.verifier, r.models, []string{"duplicate", "duplicate"}); err == nil {
		t.Fatal("duplicate legacy identity accepted")
	}
	reused, _ := NewNativeAuthorizer(bytes.Repeat([]byte{11}, 32), nil)
	if _, err := NewRuntime(p, reused, r.verifier, r.models, nil); err == nil {
		t.Fatal("persistent native subject secret reused as rotating signing key")
	}
	for _, value := range []any{r, r.native, r.verifier, &Principal{}} {
		for _, debug := range []string{fmt.Sprint(value), fmt.Sprintf("%#v", value)} {
			if !strings.Contains(debug, "redacted") || strings.Contains(debug, "owned-gateway-transit") || strings.Contains(debug, strings.Repeat("s", 32)) {
				t.Fatal("private runtime debug was not redacted")
			}
		}
	}
}

func TestRuntimeAliasesShareOneAuthorizedPoolWithoutLosingSignedSource(t *testing.T) {
	base, probe := runtimeFixture(t)
	models := append(append([]RuntimeModel(nil), base.models...), RuntimeModel{Alias: "qwen-alt", ModelTarget: nativeTargets()[0]})
	runtime, err := NewRuntime(probe, base.native, base.verifier, models, []string{"owned-gateway-transit"})
	if err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"qwen", "qwen-alt"} {
		_, c, _, _ := delegationFixture(t)
		body := []byte(`{"model":"gpu0/qwen"}`)
		n := uint64(time.Now().UnixMilli())
		c.SourceAlias = alias
		c.AllowedAliases = []string{alias}
		c.BodyHash, _ = RequestDigest(Chat, body)
		c.IssuedAtMS = n
		c.ExpiresAtMS = n + 60000
		c.QueueDeadlineMS = n + 45000
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.SetRequestURI(GatewayPrefix + string(Chat))
		ctx.Request.Header.SetMethod("POST")
		ctx.Request.SetBody(body)
		ctx.Request.Header.Set(DelegationHeader, delegationFixtureToken(t, c, nil))
		before := probe.calls
		runtime.WrapHTTP(func(h *fasthttp.RequestCtx) {
			bf := nativeGrantContext("owned-gateway-transit", "service", nativePermits(true))
			h.VisitUserValuesAll(func(k, v any) { bf.SetValue(k, v) })
			result := runtime.Handle(bf, &schemas.BifrostRequest{RequestType: schemas.ChatCompletionRequest, ChatRequest: &schemas.BifrostChatRequest{Provider: "gpu0", Model: "qwen"}})
			if result.Error != nil || !reflect.DeepEqual(probe.principal.Authorization().Pools, []string{"qwen"}) {
				t.Fatal("same-pool alias lost signed source or acquired extra capacity authority")
			}
		})(ctx)
		if probe.calls != before+1 {
			t.Fatal("alias failed to reach authorized producer")
		}
	}
	bad := append(append([]RuntimeModel(nil), models...), RuntimeModel{Alias: "duplicate-authority", ModelTarget: ModelTarget{Pool: "other-pool", Provider: "gpu0", Model: "qwen"}})
	if _, err := NewRuntime(probe, base.native, base.verifier, bad, nil); err == nil {
		t.Fatal("one wire model acquired two pool authorities")
	}
}

func TestGatewayRuntimeRetainsOriginalQueueDeadlineWithoutDispatch(t *testing.T) {
	runtime, probe := runtimeFixture(t)
	ctx := &fasthttp.RequestCtx{}
	body := []byte(`{"model":"gpu0/qwen"}`)
	ctx.Request.SetRequestURI(GatewayPrefix + string(Chat))
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody(body)
	ctx.Request.Header.Set(DelegationHeader, runtimeToken(t, Chat, body))
	runtime.WrapHTTP(func(h *fasthttp.RequestCtx) {
		bf := nativeGrantContext("owned-gateway-transit", "service", nativePermits(true))
		h.VisitUserValuesAll(func(k, v any) { bf.SetValue(k, v) })
		result := runtime.handleAt(bf, &schemas.BifrostRequest{RequestType: schemas.ChatCompletionRequest, ChatRequest: &schemas.BifrostChatRequest{Provider: "gpu0", Model: "qwen"}}, time.Now().Add(2*time.Minute))
		if result.Error == nil || result.Error.StatusCode == nil || *result.Error.StatusCode != 408 || probe.calls != 0 || result.Error.AllowFallbacks == nil || *result.Error.AllowFallbacks {
			t.Fatal("queued proof gained a fresh deadline, dispatch or misleading authentication status")
		}
	})(ctx)
}

func TestAbsoluteAdmissionDeadlineSurvivesHealthAndPreservesOriginalWeight(t *testing.T) {
	original := fixtureRequest()
	deadline := time.UnixMilli(time.Now().Add(time.Second).UnixMilli())
	var queuedAt atomic.Int64
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/health":
			time.Sleep(75 * time.Millisecond)
			ready(w)
		case "/v1/enqueue":
			var in struct {
				Request    Request `json:"request"`
				Weight     uint32  `json:"weight"`
				DeadlineMS int64   `json:"deadline_ms"`
			}
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
				t.Error(err)
			}
			if in.Request.ID != original.ID || in.Request.Owner != original.Owner || in.Weight != 1000 || in.DeadlineMS != deadline.UnixMilli() {
				t.Error("original identity, signed weight or deadline changed")
			}
			queuedAt.Store(time.Now().UnixMilli())
			queued(w, original.ID, deadline)
		case "/v1/poll":
			respond(w, map[string]any{"protocol": 1, "decision": map[string]any{"status": "granted", "lease": Lease{ID: original.ID, Pool: "qwen", Engine: "q2", State: "prepared"}}})
		case "/v1/cancel":
			cancellation(w)
		default:
			t.Error("unexpected request")
			w.WriteHeader(500)
		}
	})
	ticket, err := client.AcquireUntil(context.Background(), original, 12, 1000, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if !ticket.Deadline.Equal(deadline) || deadline.UnixMilli()-queuedAt.Load() > 950 {
		t.Fatal("health wait was not charged to the same original budget")
	}
	if _, err = ticket.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAbsoluteAdmissionRejectsEvenOneMillisecondExtension(t *testing.T) {
	original := fixtureRequest()
	deadline := time.Now().Add(time.Second)
	var polls, cancels atomic.Int32
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/health":
			ready(w)
		case "/v1/enqueue":
			queued(w, original.ID, deadline.Add(time.Millisecond))
		case "/v1/poll":
			polls.Add(1)
			w.WriteHeader(500)
		case "/v1/cancel":
			cancels.Add(1)
			cancellation(w)
		}
	})
	_, err := client.AcquireUntil(context.Background(), original, 1, 1, deadline)
	if !errors.Is(err, ErrUnavailable) || polls.Load() != 0 || cancels.Load() != 1 {
		t.Fatalf("extended queue accepted or owner lost: %v polls=%d cancel=%d", err, polls.Load(), cancels.Load())
	}
}

func TestAbsoluteAdmissionRefusesLegacyControllerBeforeEnqueue(t *testing.T) {
	var mutations atomic.Int32
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/health" {
			mutations.Add(1)
			w.WriteHeader(500)
			return
		}
		respond(w, map[string]any{"protocol": 1, "state": "ready", "routing_enforcement": "enabled"})
	})
	_, err := client.AcquireUntil(context.Background(), fixtureRequest(), 1, 1, time.Now().Add(time.Second))
	if !errors.Is(err, ErrNotEnforced) || mutations.Load() != 0 {
		t.Fatal("legacy controller created a resettable queue")
	}
}

func TestExpiredAbsoluteAdmissionHasNoIO(t *testing.T) {
	var calls atomic.Int32
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) })
	_, err := client.AcquireUntil(context.Background(), fixtureRequest(), 1, 1, time.Now().Add(-time.Millisecond))
	if !errors.Is(err, ErrExpired) || calls.Load() != 0 {
		t.Fatal("expired ingress budget made control I/O")
	}
}

func TestPreparedTicketCannotStartAfterItsOriginalDeadline(t *testing.T) {
	r := fixtureRequest()
	body := []byte(`{"model":"qwen","messages":[]}`)
	r.BodyHash, _ = RequestDigest(Chat, body)
	var data, cancels atomic.Int32
	client := fixtureClient(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/v1/cancel" {
			cancels.Add(1)
			cancellation(w)
			return
		}
		data.Add(1)
		w.WriteHeader(500)
	})
	ticket := &Ticket{Lease: Lease{ID: r.ID, State: "prepared"}, Deadline: time.Now().Add(-time.Millisecond), client: client, request: r}
	_, err := ticket.Forward(context.Background(), Chat, body)
	if !errors.Is(err, ErrExpired) || data.Load() != 0 || cancels.Load() != 1 {
		t.Fatalf("expired ticket attempted inference or lost cleanup: %v data=%d cancel=%d", err, data.Load(), cancels.Load())
	}
}

func TestFrozenProducerInputDebugDoesNotExposeBody(t *testing.T) {
	input := FrozenInput{api: Chat, body: []byte("private-prompt-image-content")}
	for _, value := range []any{input, &input} {
		if strings.Contains(fmt.Sprintf("%v %+v %#v", value, value, value), "private-prompt") {
			t.Fatal("producer policy debug leaked model input")
		}
	}
}

// The HTTP boundary sees the original model before any transport/core routing.
func TestNativeHTTPOriginalModelCannotBeReplacedByHookRouting(t *testing.T) {
	for _, tc := range []struct {
		name     string
		all      bool
		provider schemas.ModelProvider
		model    string
		denied   bool
	}{
		{"source remains DS after permitted rewrite", true, "gpu0", "qwen", false},
		{"denied DS cannot be laundered through Qwen", false, "gpu0", "qwen", true},
		{"owned source cannot escape through unowned provider", true, "unowned", "other", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime, probe := runtimeFixture(t)
			h := &fasthttp.RequestCtx{}
			h.Request.SetRequestURI("/openai" + string(Chat))
			h.Request.Header.SetMethod("POST")
			h.Request.SetBodyString(`{"model":"gpu1/ds","temperature":0.2}`)
			runtime.WrapHTTP(func(h *fasthttp.RequestCtx) {
				ctx := nativeGrantContext("employee-key", "employee", nativePermits(tc.all))
				h.VisitUserValuesAll(func(k, v any) { ctx.SetValue(k, v) })
				if !runtime.RequiresManaged(ctx) {
					t.Fatal("original owned model lost the managed boundary before routing")
				}
				result := runtime.Handle(ctx, &schemas.BifrostRequest{RequestType: schemas.ChatCompletionRequest, ChatRequest: &schemas.BifrostChatRequest{Provider: tc.provider, Model: tc.model}})
				if tc.denied {
					if result.Error == nil || probe.calls != 0 {
						t.Fatal("routing bypassed original or actual-target authorization")
					}
				} else if result.Error != nil || probe.principal == nil || probe.principal.Source().Alias != "ds" {
					t.Fatal("post-hook model replaced original model authority")
				}
			})(h)
		})
	}
}

func TestNativeLogicalAliasAndSDKIntentAreFrozenBeforeRouting(t *testing.T) {
	base, probe := runtimeFixture(t)
	models := append(append([]RuntimeModel(nil), base.models...), RuntimeModel{Alias: "qwen-thinking", ModelTarget: nativeTargets()[0]})
	aliases := []NativeModelAlias{{Provider: "gpu0", Model: "qwen-thinking-wire", SourceAlias: "qwen-thinking"}}
	runtime, err := NewRuntime(probe, base.native, base.verifier, models, nil, aliases...)
	if err != nil {
		t.Fatal(err)
	}
	aliases[0].SourceAlias = "ds" // deployment input cannot mutate the immutable runtime
	permits := nativePermits(true)
	permits[0].AllowedModels = schemas.WhiteList{"qwen", "qwen-thinking-wire"}
	for _, httpIngress := range []bool{false, true} {
		for _, qualified := range []bool{false, true} {
			t.Run(fmt.Sprintf("http=%t/qualified=%t", httpIngress, qualified), func(t *testing.T) {
				ctx := nativeGrantContext("employee-key", "employee", permits)
				req := &schemas.BifrostRequest{RequestType: schemas.ChatCompletionRequest, ChatRequest: &schemas.BifrostChatRequest{Provider: "gpu0", Model: "qwen-thinking-wire", Params: &schemas.ChatParameters{Temperature: new(0.2)}}}
				exercise := func() {
					runtime.CaptureRequest(ctx, req)
					req.ChatRequest.Model = "qwen"
					req.ChatRequest.Params.Temperature = new(0.9)
					result := runtime.Handle(ctx, req)
					if result.Error != nil || probe.principal == nil {
						t.Fatal("registered native logical alias was disabled")
					}
					p := probe.principal
					if p.Source().Alias != "qwen-thinking" || p.OriginalRoute().Provider != "gpu0" || p.OriginalRoute().Model != "qwen-thinking-wire" || p.RoutedTarget() != nativeTargets()[0] || string(p.controls["temperature"]) != "0.2" {
						t.Fatal("routing replaced original alias, controls or target identity")
					}
					// A different SDK call on the same context gets fresh small intent,
					// while an HTTP request's original snapshot remains authoritative.
					if !httpIngress {
						req.ChatRequest.Model = "ds"
						req.ChatRequest.Provider = "gpu1"
						runtime.CaptureRequest(ctx, req)
						req.ChatRequest.Model = "qwen"
						req.ChatRequest.Provider = "gpu0"
						result = runtime.Handle(ctx, req)
						if result.Error != nil || probe.principal.Source().Alias != "ds" {
							t.Fatal("SDK call inherited stale original model")
						}
					}
				}
				if !httpIngress {
					exercise()
					return
				}
				h := &fasthttp.RequestCtx{}
				h.Request.SetRequestURI("/openai" + string(Chat))
				h.Request.Header.SetMethod("POST")
				name := "qwen-thinking-wire"
				if qualified {
					name = "gpu0/" + name
				}
				h.Request.SetBodyString(`{"model":"` + name + `","temperature":0.2}`)
				runtime.WrapHTTP(func(h *fasthttp.RequestCtx) {
					h.VisitUserValuesAll(func(k, v any) { ctx.SetValue(k, v) })
					// A transport hook changing the body/typed request must not win.
					h.Request.SetBodyString(`{"model":"gpu1/ds","temperature":0.9}`)
					exercise()
				})(h)
			})
		}
	}
}

func TestNativeAliasRegistryAndActualTargetMustRemainAuthorized(t *testing.T) {
	base, probe := runtimeFixture(t)
	for _, aliases := range [][]NativeModelAlias{
		{{Provider: "gpu0", Model: "qwen", SourceAlias: "ds"}},
		{{Provider: "gpu0", Model: "logical", SourceAlias: "missing"}},
		{{Provider: "gpu0", Model: "logical", SourceAlias: "qwen"}, {Provider: "gpu0", Model: "logical", SourceAlias: "qwen"}},
	} {
		if _, err := NewRuntime(probe, base.native, base.verifier, base.models, nil, aliases...); err == nil {
			t.Fatal("ambiguous native alias registry accepted")
		}
	}
	ctx := nativeGrantContext("employee-key", "employee", nativePermits(false))
	req := &schemas.BifrostRequest{RequestType: schemas.ChatCompletionRequest, ChatRequest: &schemas.BifrostChatRequest{Provider: "gpu0", Model: "qwen"}}
	base.CaptureRequest(ctx, req)
	req.ChatRequest.Provider = "gpu1"
	req.ChatRequest.Model = "ds"
	if result := base.Handle(ctx, req); result.Error == nil || probe.calls != 0 {
		t.Fatal("original Qwen permission authorized a denied routed DS target")
	}
}

func TestNativeOriginalMetadataIsBoundedAndAmbiguousNamesDoNotPickAProvider(t *testing.T) {
	base, probe := runtimeFixture(t)
	models := []RuntimeModel{{Alias: "one", ModelTarget: ModelTarget{Pool: "one", Provider: "gpu0", Model: "same"}}, {Alias: "two", ModelTarget: ModelTarget{Pool: "two", Provider: "gpu1", Model: "same"}}}
	runtime, err := NewRuntime(probe, base.native, base.verifier, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	if origin := runtime.nativeHTTPOrigin("same"); !origin.required || origin.source != nil {
		t.Fatal("ambiguous unqualified model selected an arbitrary provider")
	}
	if origin := runtime.nativeHTTPOrigin("gpu1/same"); origin.source == nil || origin.source.Alias != "two" {
		t.Fatal("explicit original provider lost")
	}
	for _, model := range []string{"", strings.Repeat("x", 10000), "model\ncontrol"} {
		origin := runtime.nativeOrigin("gpu0", model)
		if !origin.required || origin.source != nil || origin.requested.Model != "" {
			t.Fatal("invalid or unbounded original model retained in context")
		}
	}
}

// An owned Unix fixture exercises the real managed producer and SSE decoder.
// Its trusted selected funding handle supplies the timeout, never HTTP input.
type selectedIdleFunding struct {
	typ  schemas.RequestType
	idle time.Duration
}

func (f selectedIdleFunding) Target() schemas.Route {
	return schemas.Route{Provider: "gpu0", Model: "qwen"}
}
func (f selectedIdleFunding) RequestType() schemas.RequestType                   { return f.typ }
func (f selectedIdleFunding) Bind(*schemas.BifrostContext) *schemas.BifrostError { return nil }
func (f selectedIdleFunding) StreamIdleTimeout() time.Duration                   { return f.idle }

type selectedIdlePolicy struct{ idle time.Duration }

func (p selectedIdlePolicy) Prepare(*schemas.BifrostContext, FrozenInput, *Principal) (Preparation, error) {
	return Preparation{Pools: []string{"qwen"}, HealthyEngines: []string{"q2"}, Cost: 1, NativeWeight: 1000}, nil
}
func (p selectedIdlePolicy) AuthorizeSelected(_ *schemas.BifrostContext, _ *Principal, _ Lease, typ schemas.RequestType) (Selection, *schemas.BifrostError) {
	return Selection{Target: ModelTarget{Pool: "qwen", Provider: "gpu0", Model: "qwen"}, Funding: selectedIdleFunding{typ: typ, idle: p.idle}}, nil
}
func TestManagedProducerUsesSelectedIdleTimeout(t *testing.T) {
	for _, api := range []API{Chat, Responses} {
		for _, tc := range []struct {
			name        string
			idle        time.Duration
			wantTimeout bool
		}{{"short", 10 * time.Millisecond, true}, {"long", 200 * time.Millisecond, false}, {"default", 0, false}, {"negative-default", -time.Second, false}} {
			t.Run(string(api)+"/"+tc.name, func(t *testing.T) {
				out, cleanup := ownedSelectedIdleStream(t, api, tc.idle)
				defer cleanup()
				timedOut := false
				for chunk := range out.Stream {
					if chunk.BifrostError != nil {
						if chunk.BifrostError.Error != nil && errors.Is(chunk.BifrostError.Error.Error, wire.ErrStreamIdleTimeout) {
							timedOut = true
						} else {
							t.Fatal("unexpected owned stream error")
						}
					}
				}
				if timedOut != tc.wantTimeout {
					t.Fatalf("selected idle policy ignored: timeout=%v want=%v", timedOut, tc.wantTimeout)
				}
			})
		}
	}
}

func ownedSelectedIdleStream(t *testing.T, api API, idle time.Duration) (*schemas.LLMPluginShortCircuit, func()) {
	t.Helper()
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/health":
			ready(w)
		case "/v1/enqueue":
			var in struct {
				Request  Request `json:"request"`
				Deadline int64   `json:"deadline_ms"`
			}
			_ = json.NewDecoder(r.Body).Decode(&in)
			queued(w, in.Request.ID, time.UnixMilli(in.Deadline))
		case "/v1/poll":
			var in struct {
				Request Request `json:"request"`
			}
			_ = json.NewDecoder(r.Body).Decode(&in)
			respond(w, map[string]any{"protocol": 1, "decision": map[string]any{"status": "granted", "lease": Lease{ID: in.Request.ID, Pool: "qwen", Engine: "q2", State: "prepared"}}})
		case "/v1/cancel":
			respond(w, map[string]any{"protocol": 1, "never_dispatched": false})
		case "/inference" + string(api):
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			time.Sleep(60 * time.Millisecond)
			if api == Chat {
				fmt.Fprint(w, "data: {\"id\":\"owned\",\"object\":\"chat.completion.chunk\",\"model\":\"qwen\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
			} else {
				fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"owned\",\"object\":\"response\",\"model\":\"qwen\",\"status\":\"completed\",\"output\":[]}}\n\n")
			}
		default:
			t.Error("unexpected owned route")
			w.WriteHeader(500)
		}
	})
	producer, err := NewProducer(client, selectedIdlePolicy{idle}, []schemas.ModelProvider{"gpu0"}, 4, bifrost.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx := nativeGrantContext("owned-vk", "owned-user", nativePermits(false))
	principal := &Principal{source: RuntimeModel{Alias: "sw-qwen", ModelTarget: ModelTarget{Pool: "qwen", Provider: "gpu0", Model: "qwen"}}, authorization: NativeAuthorization{SubjectHash: strings.Repeat("a", 64), Pools: []string{"qwen"}}, deadline: time.Now().Add(3 * time.Second)}
	ctx.SetValue(principalKey, principal)
	ctx.SetValue(schemas.BifrostContextKeyStreamIdleTimeout, time.Nanosecond)
	req := &schemas.BifrostRequest{RequestType: schemas.ChatCompletionStreamRequest, ChatRequest: &schemas.BifrostChatRequest{Provider: "gpu0", Model: "qwen", Input: []schemas.ChatMessage{{Role: "user", Content: &schemas.ChatMessageContent{ContentStr: new("owned")}}}}}
	if api == Responses {
		req = &schemas.BifrostRequest{RequestType: schemas.ResponsesStreamRequest, ResponsesRequest: &schemas.BifrostResponsesRequest{Provider: "gpu0", Model: "qwen"}}
		if err := json.Unmarshal([]byte(`[{"role":"user","content":[{"type":"input_text","text":"owned"}]}]`), &req.ResponsesRequest.Input); err != nil {
			t.Fatal(err)
		}
	}
	out := producer.Handle(ctx, req)
	if out == nil || out.Error != nil || out.Stream == nil {
		t.Fatal("owned dispatch rejected before timing check")
	}
	return out, func() { ctx.Cancel(); producer.Close() }
}

// A loopback-only, synthetic harness companion. It has no production config,
// secrets, GPU endpoint or native identity; no regular binary exposes this route.
func TestOwnedManagedIdleHarness(t *testing.T) {
	if os.Getenv("SW_MANAGED_IDLE_HARNESS") != "1" {
		t.Skip("explicit owned loopback harness companion only")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:19486")
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	complete := make(chan struct{})
	server := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api := API(r.URL.Path)
		if r.Method != "POST" || (api != Chat && api != Responses) {
			w.WriteHeader(404)
			return
		}
		if requests.Add(1) > 2 {
			w.WriteHeader(429)
			return
		}
		defer func() {
			if requests.Load() == 2 {
				select {
				case complete <- struct{}{}:
				default:
				}
			}
		}()
		out, cleanup := ownedSelectedIdleStream(t, api, 10*time.Millisecond)
		defer cleanup()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		for chunk := range out.Stream {
			raw, err := json.Marshal(chunk)
			if err != nil {
				t.Error("owned marshal failed")
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", raw)
			w.(http.Flusher).Flush()
		}
	})}
	go server.Serve(listener)
	select {
	case <-complete:
	case <-time.After(60 * time.Second):
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server.Shutdown(closeCtx)
}
