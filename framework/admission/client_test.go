package admission

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	respond(w, map[string]any{"protocol": 1, "state": "ready", "routing_enforcement": "enabled"})
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
		Protocol int     `json:"protocol"`
		Request  Request `json:"request"`
		Cost     int     `json:"cost"`
		Weight   int     `json:"weight"`
		WaitMS   int     `json:"wait_ms"`
	}{1, r, 1, 1, 1000}
	if err = client.call(ctx, "enqueue", input, &enqueued); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		_, _ = client.cancel(cleanup, r)
	}()
	if enqueued.Protocol != 1 || enqueued.Ticket.ID != r.ID || enqueued.Ticket.DeadlineMS <= time.Now().UnixMilli() {
		t.Fatalf("Rust enqueue contract: %+v", enqueued)
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
