# Shared physical admission protocol client

Candidate client, authorization library and nullable HTTP runtime. The factory can
compose that runtime, while the standalone CLI installs none and a qualified
production-qualified physical producer is still pending. No production configuration, Key or Grant was
changed. Prepared tickets can forward exact bytes to the local Rust sender in
owned fixtures. The existing Gateway quota ledger and Bifrost governance remain
the respective financial owners.

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

The original client-only increment had no reachable HTTP path and qualified for
the public-provider-harness exemption. The later nullable HTTP runtime below
adds wire regressions and harness cases for its now-reachable composition. Target-specific normalization, actual dispatcher CAS,
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
formula. At fixed source 0ddb997, the helper/executor had no reachable HTTP
factory path, so that historical increment qualified for the provider-harness
exemption. The nullable runtime below now installs a guarded HTTP composition
and ships corresponding entrance/auth/usage wire regressions and harness cases.


## Gateway original-caller delegation (candidate, nullable HTTP runtime)

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
are still mandatory. The HTTP factory now accepts an immutable nullable runtime composition. The standalone CLI still constructs none; a qualified physical producer and deployment loader remain pending.

Do not globally reclassify a shared legacy VK as Gateway-only without its
consumer inventory. A signed Gateway-only channel must distinguish employees
from legitimate native service identities while preserving existing keys and
financial owners. The source-to-wire-model mapping and all route shapes still need complete deployment qualification before cutover. Earlier fixed-source verifier evidence was component-only; the runtime below now adds real HTTP factory wiring and corresponding provider-harness cases. Optional Gateway signature sending is not physical routing enforcement.

Fixed-source qualification: verifier code `4fb6de9fd2e074c4598b799efd1e52e8b9e4d601`
and Gateway source `e70b0a7ffa3e55d73a84fa854133e50fd593688f` passed the
[owned actual HTTP cross-language fixture](evidence/gateway-delegation-paired-20261102.json).
Two ordinary Chat requests with the same caller trace produced two original
Gateway ledger rows; server-owned root IDs remained distinct, forged caller
tokens were ignored, and Go preserved the signed model intersection and original
Limits. This fixture does not exercise native Bifrost HTTP handlers, Responses
or streaming delegation. The [verifier regression receipt](evidence/gateway-delegation-verifier-20261102.json)
records 32 admission race pass events (three opt-in probes skipped separately),
10 real-governance SDK regressions, vet and trace-header redaction. Policy caps
are signed and verified, but their final routing enforcement remains pending.


## Nullable HTTP runtime and original-caller boundary

`Runtime` composes the verified Gateway channel and native original-Grant
intersection with the existing managed SDK boundary. The server passes the same
immutable runtime at initialization/reload, avoids a typed-nil interface, and uses
`ManagedRouterHandler` before canonical routing. No JSON/UI option, standalone
CLI loader or physical producer is installed by this increment. Production stays
on the previously qualified versions; default native requests keep their route.

The reserved `/sw/gateway/v1/chat/completions` and `/sw/gateway/v1/responses`
require exact signed bytes and explicit registered provider/model. Successful
validation reuses the existing OpenAI parser, original governance and serializers;
it does not implement another model HTTP stack. A delegation token on ordinary
native endpoints is rejected, preventing a misconfigured Gateway from becoming a
broad native service identity. Query/provider/key/compat/async overrides are not
accepted on the signed channel. Native explicit sessions are captured on the two
OpenAI inference path families; unsupported shapes conservatively remain home-only.
Only small immutable proof/principal handles enter context, never input/images or
the token. Secret-bearing debug handles are redacted.

The resolved VK row must still match the signing slot. Employee scope intersects
that VK without replacing its original Limits. Shared legacy service IDs retain
their native source-model access but cannot spill without delegation; they are
not globally disabled or reclassified. Registration must cover every model on an
owned provider before activation. Multiple aliases may share one identical pool
and wire model; they cannot manufacture another pool authority or lose the
signed source alias when deduplicating targets. A permitted pool is an authorization result,
not capability, quality, target-budget/key or actual execution approval.

Two real bypasses were reproduced before repair: context-pinned requests to an
unowned provider made one native HTTP call and zero managed calls, and pre-hook
shortcuts returned successful replies without a provenance policy check. Core now
tracks context-pinned work even if a parser/hook selects an unowned provider and
consults optional `ManagedShortcutPolicy` on successful managed pre-hook results.
Ordinary hook errors keep their original status. The current runtime **rejects
unqualified cached replies with 503** because no trusted receipt binds their
actual target/scope/registry generation yet. Existing unconfigured caching is
unchanged. Cached-result compatibility and a profile that forbids independent
model execution inside pre-hooks are mandatory before production activation;
this is a safety fence, not completed cache support.

Owned tests use actual canonical HTTP routes, server handler composition, core,
Grant, governance and response/SSE conversion, with an in-process synthetic
managed producer. Native and signed Gateway × Chat and Responses × unary and
cancelled SSE preserve nonzero original money/token/request counters exactly
once, without provider-key selection. No Rust sender, GPU or production data is
in this HTTP fixture. A test initially read the transport-owned response while
its serializer populated defaults; expected pricing now uses an independent
immutable value. This was a fixture race, not a claimed production cost fix.

Five `managed-provenance` provider-harness cases pin missing-proof unary/streaming
routes and wrong-channel token rejection; augment/filter validation selects all
five and preserves existing folders. No live provider harness was launched. Its
operator commands (pick an unused port and the standard profile) are:

```sh
make dev PORT=18493 APP_DIR=$(pwd)/tests/integrations/python
make run-provider-harness-test PROVIDER=openai FEATURE="managed-provenance" BASE_URL=http://localhost:18493
```

Still required: concrete physical producer, immutable common registry/contracts,
one original queue deadline and weight, durable CallID consumption, target
finance/key/context/quality checks, common images before physical slots, cache
receipt compatibility, async/WebSocket and other native shapes, engine abort and
crash/unknown financial recovery, declaration loader, full-source release and
rollback qualification. Do not enable this composition merely because its
synthetic HTTP and authorization tests pass.

Fixed runtime source `8b0d3e47b4978fcba7f36b90749e00decaac0a7d` passed
50 admission race events (three separately owned probes skipped), 26 SDK/HTTP
events, 18 selected core events, 1099 schema events and vet. The eight HTTP
positive shapes passed three repetitions (24 leaf cases); six denial shapes
kept original counters unchanged. The [runtime receipt](evidence/managed-http-provenance-20261102.json)
keeps these synthetic HTTP proofs separate from the [new paired Gateway/Go
verifier proof](evidence/gateway-delegation-paired-8b0d3e4-20261102.json), which
uses two actual Gateway HTTP calls and two original ledger rows but does not
traverse the Bifrost HTTP factory. Both disposable paired containers were removed.

## Absolute queue budget and upgrade-safe sender claim

`Client.AcquireUntil` carries an ingress-owned absolute deadline across health,
Unix transport and database lock waits. Gateway adapters must use the verified
original deadline and CallID; `Acquire(remaining)` is only a convenience for the
phase that first owns a fresh budget. It must not be used to restart another
phase's wait. The client requires the controller's absolute-deadline capability,
rejects even a one-millisecond extension, preserves signed weights up to1000,
and cancels an expired never-dispatched ticket before sending inference bytes.
The production `not_enabled` activation fence is unchanged.

The controller persists the first absolute bound and rejects replay that changes
it or switches between absolute/relative modes. It checks the database clock
again after obtaining its serialized lock, before inserting a queue position.
Preparing a lease clamps its TTL to that original bound. More importantly, both
sender/body checks and the atomic dispatch claim re-read the persisted queue;
a pre-upgrade lease retaining the old15-second TTL cannot start overdue work.
Polling expires only never-dispatched preparation. Dispatched/uncertain work
still belongs to the sender until actual terminal evidence, not queue expiry.
The additive nullable column preserves the legacy relative interface for old
local callers; no existing quota/ledger/credential rows are replaced.

`test-gateway-delegation.py --qualify-control` now runs original real PG/Redis
regressions in a separate owned database, explicit100-request capacity/fault
qualification, actual Go/Rust control and64KiB-content mock inference probes,
and the independent two-request original-ledger verifier proof. Its temporary
PostgreSQL readiness uses TCP, which is unavailable on initdb's temporary Unix
server. The Go probe establishes the first absolute queue itself; rebinding an
already-created relative queue must fail. All fixture sockets/DBs are owned and
both containers are removed; no production credentials or real model is used.

This increment changes the private peer-UID Unix protocol. No public provider
HTTP route is registered to this client; the public paid provider harness cannot
exercise it and is exempt. The earlier public provenance cases remain intact.
A concrete managed producer/loader, original-call binding at every entrance,
target registry/finance/contracts, common images/cache receipts, actual abort and
crash financial recovery, and complete release/rollback are still required.

Frozen client source `88f7fe194c8324958067ac9061f6c34a30a50ad8` passed55
admission race events (three opt-in probes skipped in the ordinary suite),26
original-governance SDK/HTTP events and vet. All three owned Go probes are run
explicitly by the combined fixture. Gateway fixed source
`709f1c4c362fdcb31925b021eb112c0f30bb92eb` passes121 real PG/Redis cases/6
ordinary opt-in ignores, explicit capacity100/fault/upgrade-TTL cases, actual
Go/Rust control and mock dispatch, and the separate two-row original ledger
proof. See the [summary](evidence/absolute-queue-deadline-20261102.json) and
[owned receipt](evidence/absolute-queue-owned-20261102.json).


## Concrete Unix producer and canonical HTTP proof

`Producer` now reuses the existing OpenAI Chat/Responses converters, request
serializer, file-URL resolver and SSE decoders. It freezes the actual bytes,
restores original explicit controls after source conversion, and asks a required
preparation policy to inspect a read-only `FrozenInput`. The policy cannot expand
the principal's model pools; the controller still intersects its approved graph.
Signed Gateway CallID/deadline/weight are retained. Native HTTP captures controls
and deadline before hooks; SDK-only composition starts at its managed boundary.
Input debug formatting is redacted and no body is copied into shared context.

The signer acquires before actual Unix I/O, then sends exact-body-bound owner
headers to the fixed local sender. Accepted decoding uses a bounded independent
context, retaining final usage after the downstream leaves. Existing decoders use
a private opt-in final-chunk marker; core runs the original post-hooks once.
Ordinary provider chunks and JSON are unchanged. Stream output closes only after
its bounded cleanup and operation resources finish, so core shutdown joins them.
The producer does not select a second provider key or run a second governance
pipeline. A planning error, wider pool or reused signed call cannot start native
fallback. Planning is subject to the original deadline and must not independently
execute models; common image processing belongs before holding physical slots.

Rust `start_enforced` is an explicit application composition for a **qualified**
managed ingress profile with a real matching dispatcher. Default `start` and
`start_with_sender` continue reporting `not_enabled`; the CLI installs none.
The test invokes `start_enforced` only in its owned namespace. Do not infer that
production's other entrances or policies became qualified from that test.

Fixed Gateway `012134bdc1d2d6492c1894bfdbe43a869d18a4f7` and Bifrost
`05b40f4b5c6c28d5c74e6d61a5138ad171568666` now prove **one request** through
canonical Bifrost HTTP -> original Grant/governance -> concrete producer -> real
Unix controller/dispatcher -> synthetic engine. Eight native/signed Gateway x
Chat/Responses x unary/cancelled SSE shapes preserve nonzero original money,
token and request counters once, with no provider-key selection. They finish
physical work, reach active0 and remove the owned schema. The combined helper
also runs original121 PG/Redis regressions (6 ordinary opt-in ignores), capacity100,
all4 explicit Go probes and independent actual Rust Gateway HTTP/verifier2-row
proof, and removes both containers. That last Gateway request is still separate
from the Bifrost/physical request; a single request across both frontends remains
unqualified. [Summary](evidence/concrete-producer-20261102.json),
[owned receipt](evidence/concrete-producer-owned-20261102.json).

Final local race:56 admission events/3 ordinary skips,38 SDK/HTTP events/1
ordinary owned-profile skip, targeted marker/dialer2 and vet pass. The public-IP
error test now injects a real dial failure rather than assuming TEST-NET cannot
be intercepted on the host. Full utilities are **not all green**:735 events pass,
one native TCP cancellation-pool stress test fails. An overlay of the old
utils.go also fails1 of3 repetitions, including EADDRNOTAVAIL under repeated
cancellation load; the earlier EOF cause is not fully established. Preserve both
logs and qualify the native pool under isolated Linux stress before claiming
that recovery complete. This is not attributed to the managed producer merely
because it was found during its checks.

No standalone HTTP request can configure this embedded producer yet; its paid
public-provider harness path remains unavailable, so this increment uses the
owned canonical HTTP fixture. The earlier5 public provenance cases remain.
Still needed before release: actual production registry/health/target finance/key
policy and loader; native original logical-model binding around rewriting hooks;
existing routing compatibility (do not silently disable it); combined real
Gateway frontend; common images, cache receipts, abort/crash financial recovery,
fixed Linux release and rollback. Do not substitute disabled existing features
for the requested preserved behavior. Production remains unchanged.

## Native original-model binding before routing (isolated qualification)

Bifrost source `7976a9a8d66bd6dfd708ac42ce989bde2b6e6d7f` captures bounded
original native model/controls/deadline before core `PreRequestHook` and session
routing. HTTP takes its authoritative snapshot before transport hooks; SDK
calls take it before core hooks. Rewrites retain the original `Principal.Source`
and `OriginalRoute`, while `RoutedTarget` reports the separately authorized
post-hook target. Original logical-model access, canonical source access and
actual-target access must all pass the existing Grant. Original managed intent
cannot escape to an unowned provider. No new Grant, keys or financial counters
are substituted; a preparation policy must still enforce the original source's
quality/capability contract and the controller's fixed graph.

`NewRuntime` accepts optional immutable `NativeModelAlias` bindings. Register
logical aliases explicitly instead of treating a hook's target as the user's
source. Ambiguous providerless names fail closed, malformed/oversized names
are not retained in context, and nil-context SDK calls do not place per-caller
metadata on the shared core context. The deployed read-only observation found
one enabled rule, `gpu0-qwen36-thinking-alias`, translating the Qwen thinking
name to the same canonical Qwen backend, without key pins or fallbacks. Its
production equivalent needs an explicit binding to the correct thinking source
contract; do not disable this rule to qualify the new runtime.

Red/green includes original native HTTP source/target authorization and four
SDK Chat/Responses unary/streaming failures with old core hook invocation
(overlaid only `core/bifrost.go`, not a clean old checkout). New code passes
67 admission race events/3 ordinary owned skips, 47 HTTP/SDK events/1 ordinary
owned skip, 9 managed-core events, and vet. Eight new native alias HTTP/SDK
shapes retain the original logical model and nonzero money/token/request
accounting once; these alias streams complete normally. Separately, fixed
Gateway `619db4cda0b54efcda1dde32ad7d45b9cf4f4fdf` with this Bifrost source
passes the real Rust controller/sender fixture's eight canonical native/signed
unary/cancelled-SSE cases, original121 PG/Redis tests, capacity100 and all four
explicit owned Go probes; active0/schema cleanup/two containers removed. The
independent real Gateway HTTP two-row proof is still a different request from
the Bifrost HTTP/physical proof.

[Summary](evidence/native-source-binding-20261102.json),
[fixed-source owned receipt](evidence/native-source-binding-owned-20261102.json).
Public paid harness remains unavailable for this embedded profile (no standalone
CLI runtime-registration path); actual HTTP fixtures cover it without paid
provider calls. No production activation or model/client/key changes. The
remaining release work listed above still applies, except native original
logical-model binding is now implemented and isolated-tested. Existing native
TCP pool stress failure is retained and was not rerun or declared solved here.

## Selected-target funding before dispatch (isolated, keyless private profile)

Fixed Bifrost `e73125b87fec6778ea014a2eb224080f03cfb05e` requires a
`SelectedTargetPolicy` as well as preparation. After acquiring the actual lease,
Producer checks the selected pool/model and original access, requires an opaque
funding approval for that exact provider/model/request type, and binds it to the
original request before data I/O. Missing/mismatched approvals and named-key
restrictions fail before dispatch. The profile uses registered keyless private
backends. Existing `allow_all_keys=false` plus an empty named-key list remains
compatible with keyless execution; production observation found this shape on
four private-provider grants, with no provider keys or named-key links. No
credential or permission migration is needed for that shape.

Governance's `CheckManagedTarget` reads current selected-target limits and
combines them with the original admitted payer snapshot, deduplicating shared
IDs. It uses the existing store checks and tracker, keeps the original Grant,
identity and Limits handles, and freezes the checked payers for final usage.
Approvals last at most two seconds and are bound after queueing. Candidate
refusals do not poison the original request's governance state. A store error
with an otherwise `allow` decision is refused. Core restores bounded trusted
managed routing before and after post-hooks (fresh nested fields per chunk),
so pricing/logging sees the selected model and retains original intent. The
producer also checks the original caller and absolute clock directly: the
BifrostContext parent-cancellation watcher alone can lag and previously let a
cancellation during selection dispatch inference.

Qualification:67 admission race events/3 ordinary owned skips,55 HTTP/SDK events/
1 ordinary owned skip,617 full governance events/1 skip,12 managed-core events,
vet and Rust fmt/strict clippy pass. Four standalone governance Chat/Responses
unary/streaming shapes prove distinct original/source/target budgets and rates
move once at the selected model's nonzero price, including late usage after
caller cancellation and a later configuration change. These are component
financial tests, not real-model overflow. Original Grant handles remain intact.
Budget/rate/deadline/store errors, foreign/expired/replaced approvals and wrong
funding target/type are refused; the parent-cancel case also passes20 repeats.
Red/green preserves the old ignored selected denial, old-core metadata loss,
cancellation scheduling race, and the initial empty-key-scope compatibility bug.

With fixed Gateway `6d5c018ceaf2eee04be1209a95e191a46a7c6ae2`, all eight
canonical native/signed HTTP-to-real-Rust cases now use actual original
Governance-issued approvals with the existing empty-key-list shape. A further
SDK selected-budget denial cancels prepared work before model I/O; the real
controller ends active0. Original121 PG/Redis tests,capacity100,all four explicit
owned Go probes and independent Gateway2-row proof pass;2 containers removed.
[Summary](evidence/selected-target-funding-20261102.json),
[owned receipt](evidence/selected-target-funding-owned-20261102.json).

Production remains unchanged. Fresh observation is two active Qwen replicas
(gpu2/3,8 sequences each) behind Nginx least_conn, GLM8 and DeepSeek36; running
llm-d containers are outside that Qwen front route. Pool `/metrics` samples one
replica, not the fleet. The old boot-recovery timer still fails on stopped
gpu4-7; its running-container guard prevents reopening them. Historical six-
replica receipts must not be used as current production topology qualification.

This reuses Bifrost's live spending checks and asynchronous tracking; it adds
no atomic money reservation or durable crash settlement. Production registry/
health/context/quality policy+loader, signed Gateway no-dispatch financial
handoff and one request across both actual frontends, common image/cache
compatibility, abort/unknown-work recovery, native TCP pool Linux stress and
full fixed Linux release/rollback remain. Public standalone CLI cannot install
this embedded runtime, so the paid harness cannot trigger the new branch;
owned actual canonical HTTP tests cover it. No paid provider calls were run.


### Historical dispatch cancellation evidence (candidate, 2026-11-02)

The original Rust controller's cancellation result checked only currently active
work. A finished request could consequently return `never_dispatched=true` and
have its queue history overwritten. Gateway source `a82955cfea4c43aa846dd4eec532cd12723154a5`
now checks historical dispatch timestamps and dispatched/uncertain/finished state
under the same namespace lock as dispatch CAS before mutating cancellation state.
The owned PostgreSQL regression reproduced the original failure, then passed
with the fix and 16 cancellation/dispatch races. Prepared cancellation stays
idempotent; completed or uncertain work is never relabelled as unexecuted.

`Ticket.ConfirmNeverDispatched` requires protocol1 plus explicit
`no_dispatch_evidence_version=2` and a non-null result. Legacy cancellation replies
are still usable for cleanup, but cannot approve a financial no-dispatch receipt.
The actual Go→Rust inference probe checks that a post-execution confirmation is
false. Eight owned negative/compatibility cases cover old and malformed replies.

See [summary](evidence/historical-dispatch-20261102.json) and
[fixed-source owned receipt](evidence/historical-dispatch-owned-20261102.json).
The qualified pair is Gateway a82955c / Bifrost 4c25832; 76 admission race pass
events/3 ordinary owned skips and 55 HTTP fixture pass events/1 ordinary owned
skip passed, with vet and strict Rust checks. The real owned Rust controller and
synthetic sender passed 100 concurrent capacity requests, 16 cancel/dispatch races,
8 Bifrost HTTP producer cases and the separate 2-call Gateway ledger fixture.
This is not yet a signed return receipt, the same request across both real HTTP
frontends, durable financial handoff or production loader activation.
