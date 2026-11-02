// Package admission implements the bounded, prepared-only local protocol used
// by a pre-queue adapter. It does not authenticate users, grant model access,
// select arbitrary URLs or certify upstream termination. A prepared ticket can
// forward its exact bytes through the local sender; no provider is contacted here.
// The controller client is not yet installed as a concrete physical producer.
package admission

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

var (
	ErrNotEnforced = errors.New("physical dispatch enforcement is not ready")
	ErrExpired     = errors.New("physical admission queue deadline exceeded")
	ErrRejected    = errors.New("physical admission request rejected")
	ErrUnavailable = errors.New("physical admission control unavailable")
)

// Request contains only adapter-owned identifiers and digests. Derive subjects
// from resolved identity; a caller's user/session/header is not an identity.
// EligiblePools must already intersect authorization, capabilities and routes.
// The controller still intersects the fixed graph and owns persistent bindings.
type Request struct {
	ID             string   `json:"id"`
	Owner          string   `json:"owner"`
	SubjectHash    string   `json:"subject_hash"`
	SessionHash    *string  `json:"session_hash"`
	BodyHash       string   `json:"body_hash"`
	Alias          string   `json:"alias"`
	EligiblePools  []string `json:"eligible_pools"`
	HealthyEngines []string `json:"healthy_engines"`
}

type Lease struct {
	ID      string `json:"id"`
	Pool    string `json:"pool"`
	Engine  string `json:"engine"`
	Foreign bool   `json:"foreign"`
	State   string `json:"state"`
}

// Ticket retains only bounded metadata. Its owner capability must never be
// logged, exposed in public response headers or replaced with x-request-id.
// Cancel only cancels never-dispatched work; it cannot release a GPU slot after
// dispatch. A dispatcher must CAS before I/O and retain all uncertain work.
type Ticket struct {
	Lease     Lease
	Deadline  time.Time
	client    *Client
	request   Request
	forwarded atomic.Bool
}

func (t *Ticket) Cancel(ctx context.Context) (bool, error) {
	return t.client.cancel(ctx, t.request)
}

// ConfirmNeverDispatched requires the controller's historical-dispatch check,
// not merely successful cleanup or absence of currently running work. Old
// protocol-v1 peers must not authorize a financial no-I/O receipt. A false or
// unavailable confirmation never permits refund, inference replay or release
// of work which may already have executed.
func (t *Ticket) ConfirmNeverDispatched(ctx context.Context) (bool, error) {
	if t == nil || t.client == nil {
		return false, ErrRejected
	}
	var reply struct {
		Protocol        int   `json:"protocol"`
		NeverDispatched *bool `json:"never_dispatched"`
		EvidenceVersion int   `json:"no_dispatch_evidence_version"`
	}
	err := t.client.call(ctx, "cancel", struct {
		Protocol int    `json:"protocol"`
		ID       string `json:"id"`
		Owner    string `json:"owner"`
	}{1, t.request.ID, t.request.Owner}, &reply)
	if err != nil {
		return false, err
	}
	if reply.Protocol != 1 || reply.NeverDispatched == nil || reply.EvidenceVersion != 2 {
		return false, ErrNotEnforced
	}
	return *reply.NeverDispatched, nil
}

type Client struct {
	socket string
	http   *http.Client
	data   *http.Client
}

// New uses only the literal local socket; ambient proxies, caller URLs and
// redirects cannot redirect this control protocol to a network endpoint.
func New(socket string) (*Client, error) {
	if !filepath.IsAbs(socket) || filepath.Clean(socket) != socket || len(socket) >= 104 || filepath.Base(socket) != "control.sock" {
		return nil, fmt.Errorf("bounded absolute control.sock path required")
	}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", socket)
		},
		MaxConnsPerHost: 8, MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second,
	}
	dataTransport := transport.Clone()
	dataTransport.MaxConnsPerHost = 128
	dataTransport.MaxIdleConnsPerHost = 8
	dataTransport.ResponseHeaderTimeout = 30 * time.Second
	dataTransport.MaxResponseHeaderBytes = 32768
	redirect := func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{
		socket: socket,
		http:   &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: redirect},
		data:   &http.Client{Transport: dataTransport, CheckRedirect: redirect},
	}, nil
}
func (c *Client) Close() {
	c.http.CloseIdleConnections()
	c.data.CloseIdleConnections()
}

// API is the canonical inference path, also part of the reservation digest.
type API string

const (
	Chat              API = "/v1/chat/completions"
	Responses         API = "/v1/responses"
	maxInferenceBytes     = 8 << 20
	maxResponseBytes      = 16 << 20
)

// ForwardResponse deliberately omits the underlying HTTP request and trailers:
// the ticket owner capability is private even to response/logging adapters.
type ForwardResponse struct {
	StatusCode int
	Header     http.Header
	Body       io.ReadCloser
}

// RequestDigest must hash the same immutable bytes later passed to Forward.
// It is domain-, method- and API-bound; hashing JSON after reserialization would
// authorize different bytes. These vectors are shared with Rust execution.rs.
func RequestDigest(api API, body []byte) (string, error) {
	if api != Chat && api != Responses {
		return "", ErrRejected
	}
	h := sha256.New()
	_, _ = h.Write([]byte("sw-llm-execution-v1\x00POST\x00"))
	_, _ = h.Write([]byte(api))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(body)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Forward sends once to the fixed Unix sender, never to a caller-supplied URL.
// ctx must belong to the bounded accounting operation, not just the downstream
// socket. Its owner must consume through final usage after client disconnect;
// closing this Body does not prove that GPU work or its quota obligation ended.
// Transport errors are ambiguous and must never trigger inference replay.
func (t *Ticket) Forward(ctx context.Context, api API, body []byte) (*ForwardResponse, error) {
	if t == nil || t.client == nil || t.Lease.State != "prepared" || len(body) == 0 || len(body) > maxInferenceBytes {
		return nil, ErrRejected
	}
	frozen := bytes.Clone(body)
	digest, err := RequestDigest(api, frozen)
	if err != nil || digest != t.request.BodyHash || !validRequest(t.request) || t.Lease.ID != t.request.ID {
		return nil, ErrRejected
	}
	if !t.Deadline.IsZero() && !time.Now().Before(t.Deadline) {
		if t.forwarded.Load() {
			return nil, ErrRejected
		}
		cleanup, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		_, _ = t.Cancel(cleanup) // never certifies completion of dispatched work
		return nil, ErrExpired
	}
	if !t.forwarded.CompareAndSwap(false, true) {
		return nil, ErrRejected
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute+30*time.Second)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/inference"+string(api), bytes.NewReader(frozen))
	if err != nil {
		cancel()
		return nil, ErrRejected
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-SW-Admission-Id", t.request.ID)
	request.Header.Set("X-SW-Admission-Owner", t.request.Owner)
	request.Header.Set("X-SW-Admission-Alias", t.request.Alias)
	response, err := t.client.data.Do(request)
	if err != nil {
		cancel()
		return nil, ErrUnavailable
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		_ = response.Body.Close()
		cancel()
		return nil, ErrUnavailable
	}
	if response.ContentLength > maxResponseBytes {
		_ = response.Body.Close()
		cancel()
		return nil, ErrUnavailable
	}
	// Retain only routing metadata. Internal capabilities and sender headers must
	// not escape in the public adapter's response or in an upstream error.
	headers := make(http.Header)
	for _, name := range []string{"Content-Type", "X-SW-Requested-Model", "X-SW-Resolved-Model", "X-SW-Resolved-Pool", "X-SW-Resolved-Engine"} {
		if value := response.Header.Get(name); value != "" {
			headers.Set(name, value)
		}
	}
	return &ForwardResponse{StatusCode: response.StatusCode, Header: headers,
		Body: &inferenceBody{body: response.Body, cancel: cancel, remaining: maxResponseBytes}}, nil
}

type inferenceBody struct {
	body      io.ReadCloser
	cancel    context.CancelFunc
	remaining int64
}

func (b *inferenceBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > b.remaining+1 {
		p = p[:b.remaining+1]
	}
	n, err := b.body.Read(p)
	if int64(n) > b.remaining {
		_ = b.Close()
		return 0, ErrUnavailable
	}
	b.remaining -= int64(n)
	if err != nil {
		_ = b.Close()
	}
	return n, err
}
func (b *inferenceBody) Close() error { b.cancel(); return b.body.Close() }

func (c *Client) call(ctx context.Context, path string, input, output any) error {
	var body io.Reader
	method := http.MethodGet
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil || len(data) > 32768 {
			return ErrRejected
		}
		body = bytes.NewReader(data)
		method = http.MethodPost
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://localhost/v1/"+path, body)
	if err != nil {
		return ErrUnavailable
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrUnavailable
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 8193))
	if err != nil || len(data) > 8192 {
		return ErrUnavailable
	}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == 403 || response.StatusCode == 413 || response.StatusCode == 422 {
			return ErrRejected
		}
		return ErrUnavailable
	}
	if err = json.Unmarshal(data, output); err != nil {
		return ErrUnavailable
	}
	return nil
}

func validDigest(s string) bool {
	return len(s) == 64 && strings.IndexFunc(s, func(r rune) bool { return !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') }) < 0
}
func validUUID(s string) bool {
	if len(s) != 36 || s == "00000000-0000-0000-0000-000000000000" {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func validRequest(r Request) bool {
	if !validUUID(r.ID) || !validUUID(r.Owner) || !validDigest(r.SubjectHash) || !validDigest(r.BodyHash) || r.SessionHash != nil && !validDigest(*r.SessionHash) || len(r.Alias) == 0 || len(r.Alias) > 128 || len(r.EligiblePools) > 32 || len(r.HealthyEngines) > 32 {
		return false
	}
	for _, group := range [][]string{r.EligiblePools, r.HealthyEngines} {
		for _, v := range group {
			if len(v) == 0 || len(v) > 128 {
				return false
			}
		}
	}
	return true
}

// Acquire waits before a provider queue/worker is held. One caller-supplied
// immutable operation and one controller deadline survive retries. A transport
// timeout is an unknown result: cleanup uses the same owner, never a new UUID.
// Until an actual dispatcher is qualified, the server's not_enabled fence
// deliberately refuses activation, including the current Rust implementation.
func (c *Client) Acquire(ctx context.Context, r Request, cost uint64, weight uint32, remaining time.Duration) (ticket *Ticket, err error) {
	if remaining < time.Millisecond || remaining > 60*time.Second {
		return nil, ErrRejected
	}
	return c.AcquireUntil(ctx, r, cost, weight, time.Now().Add(remaining))
}

// AcquireUntil carries the immutable ingress deadline through health, transport
// and store waits. A relative wait calculated in another process is insufficient:
// adding it to the controller's later clock would extend the original budget.
// IDs/owner and weight remain adapter-owned; verified Gateway CallID is the ID,
// never a newly generated identity on replay. Legacy controllers fail closed.
func (c *Client) AcquireUntil(ctx context.Context, r Request, cost uint64, weight uint32, deadline time.Time) (ticket *Ticket, err error) {
	if !validRequest(r) || cost == 0 || cost > 1_000_000_000_000 || weight < 1 || weight > 1000 || deadline.IsZero() || deadline.After(time.Now().Add(60*time.Second)) {
		return nil, ErrRejected
	}
	localDeadline := time.UnixMilli(deadline.UnixMilli())
	if !time.Now().Before(localDeadline) {
		return nil, ErrExpired
	}
	// Freeze slices/pointers so caller reuse cannot mutate a live operation.
	r.EligiblePools = append([]string{}, r.EligiblePools...)
	r.HealthyEngines = append([]string{}, r.HealthyEngines...)
	if r.SessionHash != nil {
		s := *r.SessionHash
		r.SessionHash = &s
	}
	ctx, cancel := context.WithDeadline(ctx, localDeadline)
	defer cancel()
	var health struct {
		Protocol         int    `json:"protocol"`
		State            string `json:"state"`
		Enforcement      string `json:"routing_enforcement"`
		AbsoluteDeadline bool   `json:"absolute_queue_deadline"`
	}
	if err = c.call(ctx, "health", nil, &health); err != nil {
		return nil, err
	}
	if health.Protocol != 1 || health.State != "ready" || health.Enforcement != "enabled" || !health.AbsoluteDeadline {
		return nil, ErrNotEnforced
	}
	// Even an ambiguous enqueue is cancelled with the original owner. This is a
	// synchronous bounded cleanup, not an unbounded cancellation goroutine.
	defer func() {
		if ticket == nil {
			cleanup, done := context.WithTimeout(context.Background(), time.Second)
			defer done()
			_, _ = c.cancel(cleanup, r)
		}
	}()
	var queued struct {
		Protocol int `json:"protocol"`
		Ticket   struct {
			ID         string `json:"id"`
			DeadlineMS int64  `json:"deadline_ms"`
		} `json:"ticket"`
	}
	if !time.Now().Before(localDeadline) {
		return nil, ErrExpired
	}
	err = c.call(ctx, "enqueue", struct {
		Protocol   int     `json:"protocol"`
		Request    Request `json:"request"`
		Cost       uint64  `json:"cost"`
		Weight     uint32  `json:"weight"`
		WaitMS     int64   `json:"wait_ms"`
		DeadlineMS int64   `json:"deadline_ms"`
	}{1, r, cost, weight, max(1, time.Until(localDeadline).Milliseconds()), localDeadline.UnixMilli()}, &queued)
	if err != nil {
		return nil, err
	}
	controllerDeadline := time.UnixMilli(queued.Ticket.DeadlineMS)
	if queued.Protocol != 1 || queued.Ticket.ID != r.ID || queued.Ticket.DeadlineMS <= 0 || controllerDeadline.After(localDeadline) {
		return nil, ErrUnavailable
	}
	if controllerDeadline.Before(localDeadline) {
		var done context.CancelFunc
		ctx, done = context.WithDeadline(ctx, controllerDeadline)
		defer done()
	}
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var polled struct {
			Protocol int `json:"protocol"`
			Decision struct {
				Status string `json:"status"`
				Lease  Lease  `json:"lease"`
			} `json:"decision"`
		}
		if err = c.call(ctx, "poll", struct {
			Protocol int     `json:"protocol"`
			Request  Request `json:"request"`
		}{1, r}, &polled); err != nil {
			return nil, err
		}
		if polled.Protocol != 1 {
			return nil, ErrUnavailable
		}
		switch polled.Decision.Status {
		case "granted":
			if !time.Now().Before(controllerDeadline) {
				return nil, ErrExpired
			}
			l := polled.Decision.Lease
			if l.ID != r.ID || l.State != "prepared" || !contains(r.EligiblePools, l.Pool) || !contains(r.HealthyEngines, l.Engine) {
				return nil, ErrUnavailable
			}
			return &Ticket{Lease: l, Deadline: controllerDeadline, client: c, request: r}, nil
		case "busy":
		case "queue_expired":
			return nil, ErrExpired
		case "queue_cancelled", "binding_unavailable", "binding_incompatible":
			return nil, ErrRejected
		default:
			return nil, ErrUnavailable
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
func contains(group []string, s string) bool {
	for _, v := range group {
		if v == s {
			return true
		}
	}
	return false
}
func (c *Client) cancel(ctx context.Context, r Request) (bool, error) {
	var reply struct {
		Protocol        int   `json:"protocol"`
		NeverDispatched *bool `json:"never_dispatched"`
	}
	err := c.call(ctx, "cancel", struct {
		Protocol int    `json:"protocol"`
		ID       string `json:"id"`
		Owner    string `json:"owner"`
	}{1, r.ID, r.Owner}, &reply)
	if err != nil {
		return false, err
	}
	if reply.Protocol != 1 || reply.NeverDispatched == nil {
		return false, ErrUnavailable
	}
	return *reply.NeverDispatched, nil
}
