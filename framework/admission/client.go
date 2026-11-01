// Package admission implements the bounded, prepared-only local protocol used
// by a pre-queue adapter. It does not authenticate users, grant model access,
// select arbitrary URLs, dispatch inference or certify upstream termination.
// It is not registered in the Bifrost request pipeline yet.
package admission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
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
	Lease    Lease
	Deadline time.Time
	client   *Client
	request  Request
}

func (t *Ticket) Cancel(ctx context.Context) (bool, error) {
	return t.client.cancel(ctx, t.request)
}

type Client struct{ http *http.Client }

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
	return &Client{http: &http.Client{Transport: transport, Timeout: 2 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) Close() { c.http.CloseIdleConnections() }

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
	if !validRequest(r) || cost == 0 || weight < 1 || weight > 100 || remaining < time.Millisecond || remaining > 60*time.Second {
		return nil, ErrRejected
	}
	// Freeze slices/pointers so caller reuse cannot mutate a live operation.
	r.EligiblePools = append([]string{}, r.EligiblePools...)
	r.HealthyEngines = append([]string{}, r.HealthyEngines...)
	if r.SessionHash != nil {
		s := *r.SessionHash
		r.SessionHash = &s
	}
	localDeadline := time.Now().Add(remaining)
	ctx, cancel := context.WithDeadline(ctx, localDeadline)
	defer cancel()
	var health struct {
		Protocol    int    `json:"protocol"`
		State       string `json:"state"`
		Enforcement string `json:"routing_enforcement"`
	}
	if err = c.call(ctx, "health", nil, &health); err != nil {
		return nil, err
	}
	if health.Protocol != 1 || health.State != "ready" || health.Enforcement != "enabled" {
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
	err = c.call(ctx, "enqueue", struct {
		Protocol int     `json:"protocol"`
		Request  Request `json:"request"`
		Cost     uint64  `json:"cost"`
		Weight   uint32  `json:"weight"`
		WaitMS   int64   `json:"wait_ms"`
	}{1, r, cost, weight, time.Until(localDeadline).Milliseconds()}, &queued)
	if err != nil {
		return nil, err
	}
	deadline := time.UnixMilli(queued.Ticket.DeadlineMS)
	if queued.Protocol != 1 || queued.Ticket.ID != r.ID || queued.Ticket.DeadlineMS <= 0 || deadline.After(localDeadline.Add(time.Second)) {
		return nil, ErrUnavailable
	}
	if deadline.Before(localDeadline) {
		var done context.CancelFunc
		ctx, done = context.WithDeadline(ctx, deadline)
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
			l := polled.Decision.Lease
			if l.ID != r.ID || l.State != "prepared" || !contains(r.EligiblePools, l.Pool) || !contains(r.HealthyEngines, l.Engine) {
				return nil, ErrUnavailable
			}
			return &Ticket{Lease: l, Deadline: deadline, client: c, request: r}, nil
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
