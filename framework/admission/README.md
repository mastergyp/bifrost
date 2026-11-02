# Shared physical admission protocol client

Candidate client and authorization library. No HTTP server factory installs a
managed executor yet; no production configuration, Key or Grant was changed.
An optional core SDK boundary and real-governance fixture now exist below.
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


## Optional SDK boundary and original caller authorization

`BifrostConfig.ManagedInference` is a fixed core execution boundary after **all**
LLM pre-hooks and before provider enqueue, rather than an orderable plugin. Nil
preserves the original route. Owned inference must return exactly one correctly
shaped result; nil, multiple results, stream/unary mismatch and managed errors
fail closed without native fallback or provider I/O. ListModels remains read-only.
The dispatcher must handle unsupported operations explicitly. Generic realtime,
raw HTTP paths and transport activation are **not** qualified by this boundary.

Core registers owned work before pre-hooks, fences new work at shutdown and waits
for accepted streams' original post-hooks before governance cleanup. A trusted
producer marks its actual final chunk with `FinalForAccounting` (never serialized).
Core, not a plugin, stamps the reserved final flag. Caller cancellation stops
socket delivery while late terminal usage still reaches the admitted hooks.
Implementations must retain their own bounded execution lifetime and may not
infer physical completion from a disconnect or this marker alone.

`NativeAuthorizer` reads the resolved original Grant. It intersects source and
candidate model permissions, retains the exact admitted Limits handle, and derives
HMAC subjects from stable key-row/user IDs. Raw key values, caller `user`, body
hashes and request IDs are not identities or conversations. Only an explicit
conversation produces a subject-scoped session. Construction requires a persistent
internal secret of at least 32 bytes; zero-value authorizers reject requests.
Configured transit credential IDs require a separate verified delegation path:
a broad Gateway forwarding key must never become the employee's authorization.
This helper does **not** validate target budgets, provider-key constraints,
capabilities, quality, health or execution completion. Those checks are required
before activation; an allowed model is not by itself an eligible route.

The real-governance fixture is a separate module with explicit local replacements:

```sh
GOTOOLCHAIN=go1.27.0 go test -C tests/managedadmission -race ./...
GOTOOLCHAIN=go1.27.0 go test -C tests/managedadmission -race github.com/maximhq/bifrost/plugins/governance
```

It uses local core/Grant/governance code and a **synthetic** managed producer,
not Rust, HTTP transport, real models, production data or credentials. Chat and
Responses, ordinary and cancelled SSE, retain nonzero original budget, token and
request counters exactly once. Unknown credentials, denied models, transit
credentials and exhausted budgets never reach provider/key selection. Earlier
Go/Rust evidence remains a separate transport proof; the two are not a combined
all-entrance release qualification.

This integration exposed an actual accounting race: the async governance worker
read response metadata while core rewrote it after post-hooks. Governance now
prepares scalar cost/token facts synchronously and only persists those facts in
the worker; later core/plugin mutations cannot change that update. Pricing panic
containment remains in place. This changes no response bytes/status or cost
formula; model-access helper and the optional executor remain unreachable from
the current HTTP server factory. The public provider-harness exemption is limited
to that absence of a reachable HTTP behavior change. Actual HTTP registration
must add entrance/auth/usage wire regressions before release.


## Gateway original-caller delegation (candidate, unregistered HTTP guard)

`DelegationVerifier` verifies a short-lived, domain-separated HMAC-SHA256
`X-SW-Gateway-Delegation-Token` against the exact POST/API/body bytes. It rejects
noncanonical base64, unknown/duplicate JSON members, invalid scopes/purposes and
expired original queue deadlines. It uses `crypto/hmac.Equal`, not string equality.
The header's `-Token` suffix is already redacted by core trace rules and covered
by a regression. Verified metadata and key debug representations are redacted;
secret bytes are excluded from JSON. Original controls are bounded and copied
on read, never retained request/image bodies.

`VerifiedDelegation.Authorize` runs after existing governance. A valid signature
also needs its configured resolved Gateway VK row ID; native employee keys
cannot borrow it. Signed original aliases intersect the existing VK model scope,
alias mappings must be unique, and the admitted original Limits object stays
intact. It never broadens a Grant, replaces its identity or skips budgets. Check
before I/O and retain that admitted result for final usage: do not re-authorize
an already accepted long inference when its admission token later expires.

Verification does not consume a call. The final dispatcher must use signed
CallID as the durable one-shot request/CAS identity; generating a new request ID
for each replay is invalid. RootID is the original quota/audit correlation. A
missing explicit conversation remains nil, never a user/body/request-ID hash.
Target budgets/keys, capability/quality/context and registry-generation binding
are still mandatory. The HTTP server factory does not register this guard yet.

Do not globally reclassify a shared legacy VK as Gateway-only without its
consumer inventory. A signed Gateway-only channel must distinguish employees
from legitimate native service identities while preserving existing keys and
financial owners. The source-to-wire-model mapping and all route shapes need
real HTTP enforcement/wire regressions before cutover. This increment is a
protocol/authorization component; no current HTTP request can reach the new
verifier through the unmodified server factory, so its provider-harness exemption
is limited to that fact. Do not interpret optional Gateway signature sending as
physical routing enforcement.
