# Original-governance managed SDK fixture

This standalone Go module explicitly replaces core, framework and governance with
this checkout. It tests their real request pipeline together without changing
other module manifests or silently substituting official module-cache code.

```sh
GOTOOLCHAIN=go1.27.0 go test -C tests/managedadmission -race ./...
GOTOOLCHAIN=go1.27.0 go test -C tests/managedadmission -race github.com/maximhq/bifrost/plugins/governance
```

The managed producer is synthetic. Its seven tokens are an assertion fixture, not
GPU usage evidence. The committed local gpt-4o pricing fixture prices five input
and two output tokens; the original budget starts at 13.25 and must increment
once, tokens start at 37 and become 44, requests start at four and become five.
Chat/Responses ordinary requests and cancelled streams use the same original
Grant/limits. Denied models, unknown credentials, configured transit identities
and exhausted budgets do not enqueue native provider I/O or select provider keys.

The producer snapshots request type before starting its goroutine because the
core request object is pooled. Core shutdown joins its late final accounting hook
before governance cleanup; the real governance worker must settle the original
nonzero counters, not a new ledger or a fresh replacement Grant.

This is not HTTP transport qualification, Rust physical execution, production
credential migration, a GPU abort receipt or whole-batch release permission.
No real model, production database, account, credential or external HTTP endpoint
is supplied. Both previously recorded Go/Rust probes are separate proofs. See
../../framework/admission/README.md for remaining activation requirements.
