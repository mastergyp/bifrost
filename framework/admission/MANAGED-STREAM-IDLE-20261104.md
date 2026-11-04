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
