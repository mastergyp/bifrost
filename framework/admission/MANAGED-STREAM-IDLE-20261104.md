# Managed streams must honor the selected provider idle policy

2026-11-04. Source/local qualification, **not deployed yet**. Production is still68a57c4/image72d3/CIDa53; Gateway2687, dependencies, Keys and client packages unchanged.

The production readonly config projection has gpu0/Qwen120s, gpu1/DeepSeek120s and gpu2/GLM600s stream_idle_timeout_in_seconds. Both managed Chat and Responses handlers instead supplied hard-coded60s. The owned operation context is independent, so normal provider network settings never reached it. This is a confirmed configuration defect; it does not prove every upstream stall's GPU/queue/proxy cause.

The selected-target funding callback reads only the selected provider's existing network scalar from the live in-memory config, attaches it through an opaque wrapper, and delegates the original funding Target/RequestType/Bind unchanged. After the existing grant/target/funding checks, Producer applies that trusted selected timeout to its owned context before consuming the stream. Source/caller contexts cannot replace the selected model policy when spilling. Missing selected config fails503 before dispatch; unset/nonpositive values use the official120s default. The original20-minute operation deadline still bounds a long configured idle timeout. No production config value, queue deadline, retries/fallback, grant, accounting or physical-release rule changes.

Actual red: both owned Chat and Responses short-policy cases completed instead of honoring selected10ms (wrong assertions, no compile/panic failure). Green: eight real Unix/HTTP/SSE cases cover short/long/default/nonpositive policies and conflicting source context; server cases cover selected120/600s, default, existing lifetime cap, missing config and exact original funding/binding preservation. Complete admission package passed0.949s, targeted server policy/startup and original governance/cancelled usage regression passed; go vet clean. The existing startup fixture lacked the already-required durable_rejection_version1; its synthetic health reply was corrected without relaxing production checks. Linux Docker build now also runs the server policy/startup tests.

[Local proof](evidence/managed-stream-idle-local-20261104.json). Provider-harness folder131 adds two owned-fixture selected-idle cases, append-only; augment/filter retain both. No live harness or paid provider run. The optional companion is test-only, fixed loopback19486, bounded to two calls/60s; it uses the real Producer/Unix path/SSE decoder with synthetic funding, not production OAuth or a new finance qualification. Original finance regressions run separately. Do not point these fixture cases at production.

Optional two-terminal local harness commands (no real model credentials). Normal make dev does not install this owned private-controller fixture, so its explicit test companion is required here:

```bash
cd /Volumes/WD/shinewing-server-refactor/bifrost
SW_MANAGED_IDLE_HARNESS=1 GOWORK=/tmp/sw-managed-readiness.go.work GOTOOLCHAIN=local /Volumes/WD/shinewing-server-refactor/go-cache/mod/golang.org/toolchain@v0.0.1-go1.27.0.darwin-amd64/bin/go test ./framework/admission -run '^TestOwnedManagedIdleHarness$' -timeout 90s -v
```

```bash
cd /Volumes/WD/shinewing-server-refactor/bifrost
make run-provider-harness-test PROVIDER=openai FEATURE="selected-idle" BASE_URL=http://127.0.0.1:19486
```

Next: fixed Git archive Linux build, inspect its one retained operation and runtime metadata. Then qualify the exact current68 source/candidate and perform Bifrost-only normal drain/current-data rollback, preserving unknown physical work. Attribute new failure phases/pools and observe a new bounded traffic window after actual deployment; never use source tests as production success.


## Fixed Linux build and bounded source export

Runtime candidate is fixed9abd33f4120d45a79539ce739791636e067c6819. Original build unit `sw-bifrost-managed-idle-build-9abd33f-r1`, root `/data/releases/shinewing-bifrost-managed/9abd33f4120d45a79539ce739791636e067c6819`, intent `/tmp/sw-bifrost-managed-idle-build-intent.json`; inspect only this operation. Production remains68/a53 until a separate qualified activation.

The first full archive was560,672,783 bytes and timed out during upload. Observed original staging Python absent, exact root empty, no source/archive/build receipt and unit not-found: a known pre-build failure, not an unknown build to replay. The same original root's single stage-resume-r1 receives a compact Git archive from the same fixed runtime revision. Exporter operations4dcbfaf checks all Docker COPY inputs and rejects unknown roots/ADD/syntax; includes all core/framework/plugins/transports/UI/managed qualification code, dependency locks and static build inputs. Only unrelated docs/tests not consumed by Docker are omitted; original Docker ignore already excludes them. Three guard tests pass.

Compact source11,498,861 bytes, SHA84b7d9810c885d30f1157d02b1c23a6b391c4355eeb0e93124e36accf7055f70 (about97.95% smaller). Original full SHA153657c0e38b1f9eb15bbc8bfd57ddde6d7cd3b9e1287c2c594f98838770332d retained in the intent. Exact compact hash/paths and one resumption intent/ack recorded before the original build unit's first launch. No existing runtime, data, key, old release or client is removed. This reduces upload/retained-source waste; Go dependency cache/layer optimization remains separate.

Build is pending until original unitMainPID0/resultsuccess/exit0 and build-r1/image.json exist. New app needs fresh current68/a53 Bifrost-only preparation/activation; do not run the old constants for245/c365 or recreate original68 candidate. New failure-pool/stage attribution and future traffic validation remain open.
