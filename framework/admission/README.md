# Shared physical admission protocol client

Candidate library only. Not registered in any Bifrost plugin or request path;
no production config, worker, Key, Grant, provider or billing behavior changes.
No inference is sent by this package. The existing Gateway quota ledger and
Bifrost governance remain the respective financial owners.

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
not evidence that GPU work ended. No dispatch or finish callback exists in this
client. Retries, fallback hooks or a client disconnect cannot certify termination.

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
