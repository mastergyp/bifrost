# Managed sender Unix connection lifecycle

Production synthetic OAuth testing exposed a fast `502/provider_connection_failed`, subsequently associated with a broken pipe writing the local sender socket and a signed never-dispatched result. The error vocabulary itself is not proof of no inference: it also covers truncated streams. No production request is replayed by this change.

The managed `Producer` keeps a fasthttp connection pool for its private Unix HTTP inference hop. A peer can close an idle socket or restart before the pool's 30-second idle budget. The pool does not continuously read unused sockets. In the owned real Unix HTTP reproduction, the sender expires idle connections after 20ms; a later inference call reuses the dead socket and fails. Restarting the sender reproduces the same failure. Chat and Responses, buffered and SSE, all fail before the fix. This qualifies the transport-lifecycle fault family; it does not assert that the exact production server timeout is 20ms.

The selected/funded internal inference request now carries server-owned `Connection: close`. Each completed local inference hop leaves no idle socket for a subsequent inference call. The native GPU HTTP connection pool is unchanged. No POST retry is added, and the one-call claim, queue budget, model selection, funding, wire body, cancellation, accounting and physical-release rules remain unchanged. If the sender accepts a request and closes before responding, that request still surfaces as ambiguous failure and is not replayed. A new producer starts with an empty pool; this fix requires replacing Bifrost normally rather than changing a live pool in place.

## Qualification

- Real Producer + owned Unix HTTP: 12 cases, Chat/Responses × buffered/SSE × idle close/restart/accepted POST reply lost. Eight idle/restart cases fail red on the previous code; four reply-lost cases already preserve one accepted POST. All12 pass with the fix, including five complete repeats.
- Race run twice covers the lifecycle cases, selected-idle policy, owned-stream diagnostic and existing Core cancellation regressions; full `framework/admission` also passes.
- A loopback-only harness companion `TestOwnedManagedUnixLifecycleHarness` runs six cases per API at port19487. Actual synthetic HTTP probes of both APIs return200/passed, zero real-model calls and zero production connections. This bridge is not an employee authentication or GPU test.
- Two `managed-uds-lifecycle` provider-harness cases are inserted without reformatting the collection; augment/filter finds exactly two. The paid provider harness was not run. Existing custom profile, not upstream default, remains the production source.

This trades one local Unix dial per inference call for avoiding cross-call socket lifetime ambiguity. It adds no TCP/TLS handshake to the model and no extra inference. The impact on production throughput must be measured after deployment with the same workload; it is not certified as zero overhead or maximum GPU efficiency. No production component has been changed by this source qualification.

For the owned companion only:

```sh
cd framework
SW_MANAGED_UNIX_HARNESS=1 GOWORK=/tmp/sw-managed-idle-all.go.work go test ./admission -run '^TestOwnedManagedUnixLifecycleHarness$' -count=1
```

In a second terminal, using the newman's manually triggered harness if desired:

```sh
make run-provider-harness-test PROVIDER=openai FEATURE=managed-uds-lifecycle BASE_URL=http://127.0.0.1:19487
```

Do not point those two synthetic qualification cases at production or a paid-model server. [Local evidence](evidence/managed-unix-lifecycle-local-20261105.json).
