# Queued job state reconciliation

## Accepted scope

- Scan every minute, with a one-minute webhook grace period.
- Share bounded REST checks between background reconciliation and pre-expansion validation.
- Keep queued jobs on unknown responses, 404, permissions errors and transient failures; defer stale demand rather than infer completion.
- Advance only explicitly observed in_progress/completed states, preserving instance associations and unrelated metadata. Do not invoke runner lifecycle handlers.
- Prevent database state regression and reject expansion claims for non-queued jobs.
- Preserve the existing terminal cleanup grace period; arbitrarily delayed webhooks after deletion remain outside the guarantee.
- Do not change pool limits, provider lifecycle, or discover jobs whose queued webhook never arrived.

## Implementation decisions

- One controller-wide budget of five new job REST checks per minute (up to 300/hour),
  shared by all entities and the background/pre-expansion paths. Existing runner
  listing traffic is separate. Results are keyed by endpoint, repository and job ID.
- Cache positive observations for one minute. Concurrent callers do not duplicate
  an in-flight check. Unknown/error observations defer only the stale demand.
- Oldest active waiters reserve available slots across entities; reservations from
  callers absent for two minutes no longer block others. Unused entries expire after
  one hour. Under backlog, convergence can exceed one scan interval.
- Ten-second request timeout; per-job exponential error backoff from one to fifteen
  minutes. Honor rate-limit Retry-After/reset headers. Ordinary permission-denied
  responses do not block unrelated checks.
- REST identity must match job ID and, when known, workflow run ID. Conditional SQL
  advances only the same entity-owned queued row, leaving runner bindings unchanged.
- Claiming a job checks queued status atomically. Webhook database writes reject
  reverse transitions in the transaction. This does not prevent remote state from
  changing immediately after the last check/claim.
- Cleanup grace now uses updated_at instead of created_at so an old job newly
  marked completed is not immediately removed. No schema migration is required.

## Delivery checklist

- [x] Regression tests fail before the fix
- [x] Implement bounded checks, atomic state updates and queued claims
- [x] Focused race tests, Linux full-suite race tests and incremental lint
- [x] Review errors, concurrency, limits and workload safety
- [ ] Commit and push task changes to an authorized remote
- [ ] Back up installed binary and stopped database, deploy, verify health

## Acceptance

Cover completed/in-progress observations, genuine queued demand, stale cached consumers,
webhook regression, unknown status and HTTP errors, request budgets and shared checks,
and unchanged min-idle/max-runner behavior. A successful REST check is not proof that
the remote job cannot change immediately afterward; the fix bounds stale demand rather
than promising atomic coordination with GitHub scheduling.

## Validation (2026-09-09)

- Red test demonstrated completed-to-queued regression and an invalid expansion claim.
- Focused SQL/pool race regressions passed three consecutive runs.
- Final Windows `go test -race -mod=vendor -tags testing ./database/sql ./database/watcher ./runner/pool -count=1 -timeout=15m`: passed (58.776s / 10.026s / 112.477s).
- Final Linux Go 1.26.2 `go test -race -mod=vendor -tags testing -v -timeout=60m -parallel=4 -p=4 -count=1 ./...`: passed.
- Incremental golangci-lint against 6c8bbdf: zero issues; diff whitespace check passed.
- A separate Windows watcher whole-suite `-count=3` run encountered `producer already registered` in TestConsumeWithFilter. The final single-run Windows and Linux full regressions passed; this repeated-suite isolation issue was not expanded into the production fix.
