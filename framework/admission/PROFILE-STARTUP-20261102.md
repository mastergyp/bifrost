2026-11-03 incremental readiness fix: loaded profiles attest exact namespace
and execution contract at GET /sw/gateway/health (no-store). Unconfigured,
prototype and closed runtimes return503. Gateway checks this bounded private
channel before employee reservation. Seven owned not-ready/mismatch/malformed/
timeout cases leave ledger and RPM untouched; the same profile then passes six
Gateway success scenarios and original 7-row/72-unit accounting. Native and
ordinary employee queue weights now both default to1, independent of native
capacity reserves. [Fixed-source joint proof](evidence/managed-readiness-accounting-20261103.json).

Go admission package passes; readiness regression failed404 before the fix.
Provider harness folder130 covers unconfigured health503 without model I/O;
collection augmentation and OpenAI filtering preserve it. No paid sweep run.
This remains synthetic model/fixed authorized-user evidence, not real GPU/IdP
or production activation. Earlier artifact989 remains built, not active.

# Standalone managed-admission startup

Latest joint success qualification: Gateway `e75a3a9` / Bifrost `1c3628a`
now traverse actual Gateway HTTP (after a fixed authorized-policy fixture),
original Redis/PG employee accounting, actual Bifrost HTTP/profile/governance,
and Rust native sender. Six Gateway success scenarios include overflow,
cancelled streams, purpose-scoped vision and five-image combined accounting;
7 original employee rows/72 units/RPM7 match Redis, and 8 actual subcalls charge
56 tokens to original Bifrost governance. Direct unauthorized model access
remains403; both helper session bindings and all active leases end at0.
[Source-bound proof](evidence/gateway-purpose-accounting-20261103.json) and
[scope/remaining work](https://github.com/mastergyp/shinewing-ai-gateway/blob/codex/model-overflow-routing/docs/GATEWAY-PURPOSE-ACCOUNTING-20261103.md).
These are synthetic engines and fixed identity policy, not real IdP/GPU or
Linux managed-profile proof. Bifrost changes in this batch are test-only, so
no provider-wire harness case is required. Production fork activation remains
pending; Qwen private per-engine API/tokenizer paths are now deployed separately.

2026-11-03 current: Gateway `cf88c72` now installs the actual Rust planner/sender;
Bifrost `56f9981` requires its authority and durable-rejection handshake. The
configured native/signed HTTP qualification passed 8 synthetic inference calls,
4 pre-ticket rejection replays and 12 rejection/dispatch races with active=0.
See [source-bound evidence](evidence/planned-execution-20261103.json) and the
[Gateway integration and remaining activation gaps](https://github.com/mastergyp/shinewing-ai-gateway/blob/codex/model-overflow-routing/docs/PLANNED-EXECUTION-20261103.md).
Production Bifrost/managed routing remains unchanged. This is not public Gateway
employee-HTTP success, a Linux application image or real GPU qualification.
Earlier sections below retain their historical scope.

`BIFROST_MANAGED_ADMISSION_CONFIG=/absolute/operator-owned/profile.json` now installs the existing Runtime and Producer from the normal HTTP server bootstrap, after the original governance plugin has loaded and before core initialization. No environment setting means the original native path. An explicitly empty, invalid or incompatible profile fails startup; it never silently falls back to uncontrolled physical dispatch.

The separate [profile schema](profile.schema.json) describes this startup-only document; it is not a mutable UI/provider setting. The file must be a regular, non-symlink file, at most256KiB, without group/other write permission. Duplicate/unknown JSON fields and trailing documents are rejected. File identity is checked across open. Only `BIFROST_MANAGED_*` environment references may supply the independent native subject and delegation secrets; the loader does not create, rotate or print any user key/token. Original VK row IDs identify forwarding channels, not secret VK values. Subject material remains stable across restarts.

Example shape (synthetic authority and IDs, **not a production profile**):

```json
{
  "version": 1,
  "socket": "/run/sw-admission/control.sock",
  "namespace": "d5f6d5c9-a5fb-4bd1-9a0d-7fd4af925480",
  "contract_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "subject_key_env": "BIFROST_MANAGED_SUBJECT",
  "delegation_keys": [{
    "id": "gateway-machine-1",
    "secret_env": "BIFROST_MANAGED_SIGNING",
    "gateway_virtual_key_ids": ["resolved-transit-row-id"]
  }],
  "models": [{"alias":"sw-qwen","pool":"qwen","provider":"private-qwen","model":"native-qwen"}],
  "native_aliases": [],
  "shared_legacy_vk_ids": [],
  "delegated_credential_ids": [],
  "max_operations": 128
}
```

Startup requires the fixed Unix controller to report protocol1, ready, enforcement enabled, absolute deadline support, `preparation_policy_version:1`, and the exact configured namespace/contract digest. Different authority is refused. All control, preparation and data-sender requests carry `X-SW-Admission-Namespace` and `X-SW-Admission-Contract`; the Rust implementation must enforce these headers before it advertises the capability. Existing unconfigured clients send no new authority headers.

The preparation policy posts to the same Unix socket, never a caller URL or ambient proxy. `POST /v1/prepare` receives protocol1, canonical API, original source alias, originally authorized pools, delegated original context/output ceilings (0 for native), and the frozen JSON body. It must compute eligible targets from the immutable registry, fresh engine health, native capabilities and target tokenizer/context checks. It returns protocol, namespace, contract_sha256, pools, healthy_engines, cost and native_weight. Reply size is bounded16KiB; duplicate/unknown/trailing fields, expanded scope, unknown/duplicate pools/engines, wrong authority and invalid costs/weights fail closed. Preparation is bounded by the remaining original deadline and10seconds and uses a separate data connection pool, preserving the control pool for cancellation/health.

After queueing selects the actual target, startup composition calls the **original governance plugin's** `CheckManagedTarget` and passes that approval to Producer. The original Grant, target budgets/rates, provider/model scope and final accounting remain in place. No synthetic funding approval is installed. Service-unavailable preparation is503; invalid target contracts remain400. Runtime resources close on failed later bootstrap and all normal server-start return paths.

## Validation and current limit

The dedicated Unix HTTP tests cover file/secret/authority boundaries, actual configured Runtime construction, exact source/limit metadata, target funding denial, malicious/unavailable plans and old-controller refusal. The transport's actual startup helper is tested both unset and enabled after original governance, with repeated replacement denied. Existing original-governance HTTP/native/signed unary/cancelled-stream and signed refusal regressions also pass. These are local synthetic services, not production GPU or real IdP qualification. See [fixed evidence](evidence/profile-startup-20261102.json).

**Not activated in production.** The production Rust controller does not yet implement this preparation capability or its per-request authority enforcement, so this loader correctly refuses it. Next implement that endpoint plus the actual executable sender/target registry lifecycle, preserve Responses/tool/image semantics, connect the two real frontends, then publish an incremental production routing release. Native GLM and DeepSeek OpenAPI inspection shows only Chat/Completion tokenization; treating a Responses body as a Chat tokenize request is invalid. Preserve the existing Responses path while implementing its exact context/session contract; do not silently remove it to obtain a passing profile.

This loader is one remaining production integration step, not evidence that automatic overflow or durable financial crash recovery has shipped. The Gateway's already-deployed four-engine observation is independent and remains available.


## Linux artifact prepared, not activated

Focused source `989fe5907743c4efff46250b0d1d7344ba3900ca` builds
`sha256:14167dfe04f310efe7e8e0795ce02db8e8f731f3508a39dd9a434491f0f976bf`
using `transports/Dockerfile.managed`. The Linux admission package passes and
the compiled main binary's help exits0 in an owned network-none container,
which was removed. Original official runtime User1000:0, entrypoint, command,
environment, working directory, volumes, healthcheck and exposed ports match.
[Artifact proof](evidence/managed-image-989fe59-20261103.json).

This is compilation/local protocol proof, not full Linux managed application
startup or production activation. An optional profile-probe image and the
Gateway helper build are separate named jobs; inspect their original units,
never restart because an observation timed out. The existing full-provider
Dockerfile/proofs are retained; this increment uses admission tests only as
authorized by the user's release policy.
