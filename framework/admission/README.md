# Shared physical admission protocol client

Candidate library only. Not registered in any Bifrost plugin or request path;
no production config, worker, Key, Grant, provider or billing behavior changes.
Prepared tickets can now forward their exact bytes to the local Rust sender.
No public/provider hook calls this package yet. The existing Gateway quota ledger
and Bifrost governance remain the respective financial owners.

The client uses only a bounded literal Unix `control.sock`, with no ambient
proxy or redirects. Rust verifies its kernel peer UID. Calls have a two-second
limit and bounded JSON bodies. Before enqueueing it requires protocol 1,
`state: ready` **and** `routing_enforcement: enabled`. The current Rust server
reports `not_enabled`, so activation is deliberately refused without creating
a queue entry until a qualified actual dispatcher exists.

Acquire must run after authentication/model authorization and capability
filtering, before holding a provider worker. The adapter supplies one random
operation/owner and immutable subject/session/body digests derived from trusted
identity. No stable session means no spill. It must pass the remaining part of
the original 60-second budget; the client's loop never creates another ticket
or restarts the database deadline. It preserves the logical provider/model and
does not rewrite Grants to grant access to another provider.

A returned Ticket is prepared only. Failed/unknown enqueue, timeout and context
cancellation attempt a synchronous one-second cleanup with the same owner.
The caller must also cancel a handed-off ticket on every pre-dispatch failure.
Cancel returns whether never-dispatched work was cancelled; false is explicitly
not evidence that GPU work ended. Forward is a one-shot data request; it is not a dispatch/finish callback. Retries,
fallback hooks or a client disconnect cannot certify termination.

Tests: `cd framework && GOTOOLCHAIN=go1.27.0 go test -race ./admission` and
`go vet ./admission`. Tests cover the activation fence, one operation/deadline,
unknown enqueue cancellation, bad leases/protocol, bounded responses, redirects,
parallel owner isolation and cancellation after dispatch. The opt-in
`TestOwnedRustControlProtocol` is invoked by Gateway's owned capacity fixture
with only a synthetic request and socket; it proves the actual Go↔Rust codec,
prepared acquisition/cancel and the real activation fence with disposable PG.
No database credentials, real user data or inference endpoint reach that probe.

The public provider harness is exempt for this increment: no HTTP request can
reach this unregistered library. Hook integration must add its wire regressions
when it is connected. Target-specific normalization, actual dispatcher CAS,
terminal evidence/uncertain recovery, all entrance paths and a fixed release
remain required before deployment. This library is not whole-stack qualification.

## Data forwarding and stream cancellation

`RequestDigest(Chat|Responses, bytes)` implements the shared Rust domain/method/API
SHA256 vectors. `Ticket.Forward` clones those exact bytes, refuses a different
API/body before any I/O, and sends only once even after an ambiguous connection
failure. It uses a separate bounded Unix HTTP pool, no proxy/redirect, 8 MiB input,
16 MiB output, 32 KiB response headers and a bounded operation lifetime. Twelve
waiting inference requests leave the two-second control path responsive. Owner
headers remain private; response objects expose only status, an allowed set of
routing/content headers and a bounded body, without request or trailer objects.

The accounting operation must retain and consume that body through final usage
when the external client disconnects. Passing the external socket's cancellation
context or closing the body early would lose that financial information even
though Rust safely keeps its physical lease. This transport does not implement
that ownership transfer and cannot enable the currently disabled server fence.

Core's short-circuit streaming path now keeps executing post-hooks after its
consumer leaves. The previous blind drain lost late usage/error chunks. Red tests
reproduced missing usage before the fix; race regressions cover cancellation before
the first chunk, after the first chunk and a normal live consumer, preserving the
captured request type after caller reuse. These tests prove hook delivery, not
end-to-end governance charging or GPU abort. The change has no client-visible
payload/status effect: it processes chunks after the client already disconnected;
the generic provider HTTP harness cannot configure this test-only managed sender.
Thus this increment is exempt from a paid wire harness case. Hook registration
must add actual entrance/auth/usage wire regressions before release.

`TestOwnedRustInferenceProtocol` is a second explicit opt-in compiled Go probe.
The Rust owned fixture supplies one prepared synthetic request/body and socket in
an otherwise empty environment. It must reject Chat-to-Responses misuse, perform
one real Unix→Rust→mock-engine POST, consume the result and release the physical
lease. Ordinary Go tests skip both Rust probes; a separate receipt records their
explicit execution. No real model, production key, database or URL is supplied.

The explicit Go/Rust inference probe has now passed with core/client source
`3cd9342` and Gateway fixture `b4f34d6`: 64 KiB exact request, one mock POST,
`finished` physical lease, active zero and schema/PG removed. See
[evidence](evidence/go-rust-managed-inference-20261102.json). This is a local
transport/execution proof; no public hook, production DB or real model was used.
