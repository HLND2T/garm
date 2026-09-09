# Failed runner recovery

## Incident and scope

On 2026-09-09 at 17:12:44 UTC+08, the Windows runner reported HTTP 520
and runner status `failed`, while the provider instance remained `running`.
The min-idle calculation counted it as available; the reaper required another
15 minutes without updates before recovery. The pool had min-idle 1 / max 2.

## Decisions

- Count only pending/installing/idle runners on pending-create/creating/running
  instances toward min-idle. Failed and deleting instances still consume the
  maximum capacity until removed: no physical oversubscription.
- Explicit failed bootstrap on a running instance skips the installation grace
  on the next existing five-minute cleanup scan. Do not add GitHub polling or
  change the one-minute queued-job reconciliation interval.
- Require a successful complete GitHub listing and offline/absent remote runner
  before deletion. Online/busy/unknown runners remain protected. Use the existing
  instance lock and normal DeleteRunner path; no force removal or provider change.
- Windows GitHub bootstrap HTTP requests have a 60-second request timeout and
  at most five retries separated by five seconds. Retry 408/429 and server/network
  failures; permanent 4xx fails immediately. Downloads retain existing retry limits.
- Retry only HTTP requests, not user/service creation or the complete installer.
  Keep warning messages out of response values and do not log authorization data.
- Database templates are not automatically replaced by new binaries. Upgrade the
  pool's custom template 5 using the guarded offline script, preserving workspace
  and service customizations, then back up and upload it explicitly.

## Acceptance checklist

- [x] Red regressions reproduced failure to replenish and delayed failed cleanup
- [x] Targeted pool tests and real Windows PowerShell HTTP tests pass
- [x] Additional boundary coverage, full regression and incremental lint
- [ ] Review final diff and push to an authorized remote
- [ ] Back up binary, database and custom template; deploy and verify

HTTP retries reduce sensitivity to transient failures; they do not repair the
upstream cause of a persistent 520. There remains a distributed race between a
GitHub observation and deletion; the existing GitHub deletion refusal/error path
must be respected, rather than bypassed.

## Custom-template upgrade

Export the current template with `garm-cli template download <id> --path <backup>`.
Run `scripts/upgrade-windows-http-retry.ps1 -InputPath <backup> -OutputPath <new>`
and review the diff. The script refuses existing output files, already upgraded
templates and unexpected request counts. It is intentionally limited to templates
derived from the older GitHub Windows installer, not arbitrary PowerShell scripts.

On Windows, set `GARM_TEST_WINDOWS_TEMPLATE` to the generated file and run
`go test -race -mod=vendor ./internal/templates -run TestWindowsBootstrapHTTPRetry -count=3`.
This renders the template and executes only the HTTP helper functions against a
local test server; it never runs the installer or creates users/services. Clear
the variable afterward to test the built-in template normally.

After backup, upload with `garm-cli template update <id> --path <new>` and verify
the downloaded bytes match. To roll back the template, upload the saved original;
do not roll back the whole job database just to restore a template.

## Validation (2026-09-09)

- Red tests reproduced both min-idle starvation and delayed explicit-failure cleanup.
- Focused pool and custom-template Windows HTTP race tests passed three repetitions.
- Windows `go test -race -mod=vendor -tags testing ./database/sql ./database/watcher ./runner/pool ./internal/templates -count=1 -timeout=15m`: passed (59.370s / 10.456s / 108.882s / 5.099s).
- Linux Go 1.26.2 `go test -race -mod=vendor -tags testing -v -timeout=60m -parallel=4 -p=4 -count=1 ./...`: passed, exit 0.
- Incremental golangci-lint against 9472820f: zero issues; whitespace checks passed.
- The migration tool rejected repeated upgrades and existing output files. Reviewed
  custom-template diff retains workspace, credentials transformation and service logic.
- The original production failure recovered under the old binary around 17:32-17:33;
  this is incident evidence, not attributed to the new deployment.
