# Releasing and deploying

## Validation gates

CI runs on every pull request and pushes to `master`, checking formatting, clippy,
locked tests, the isolated EMQX/InfluxDB suite, and Compose validation. The CD workflow
calls the same CI workflow for the exact source revision it will build or
redeploy; a successful run for another commit is not a release gate. Historical
revisions that predate the broker integration harness run their available Rust
and Compose checks, but cannot run that newer broker suite.

## Release a new version

1. Update `version` in `Cargo.toml` and `Cargo.lock`, then merge the change to
   `master`.
2. Create and push a matching version tag, for example:

   ```bash
   git tag v1.2.3
   git push origin v1.2.3
   ```

3. CD validates the tagged commit before building, checks the tag against
   `Cargo.toml`, builds an ARM64 image, and publishes version and full-source-SHA
   tags with OCI source/revision labels.
4. CD resolves the pushed image to its registry digest and deploys that digest,
   not a mutable tag. It stages the Compose file from the same source revision
   and the host-side config bundle, then runs `--check-config` against the exact
   mounted bundle before replacing the running service.

The workflow does not publish or deploy `latest`.

## Build and deploy an ad-hoc revision

In **Actions -> CD -> Run workflow**, leave `image_tag` empty. CD validates the
selected workflow revision, builds an ARM64 image tagged with its full source
SHA, and deploys the resolved registry digest.

## Redeploy or roll back an existing image

In **Actions -> CD -> Run workflow**, enter either a release tag such as
`v1.2.3` or the full image tag `sha-<40-character-source-SHA>`. CD rejects
`latest`, short SHA tags, and arbitrary tags. It reads the image's source
revision label, resolves the registry digest, and reruns the validation
workflow for that revision before deployment.

Selecting an image other than the running image restores its most recently
activated, preserved Compose/config/environment pair, including a captured
pre-cutover baseline. CD refuses the rollback if no compatible pair exists.
This matters across the unified-route cutover: a pre-cutover image needs its
legacy settings and may not support `--check-config`.

Selecting the running image stages the current host configuration and `.env`.
Changes to either create a new pair and require preflight, so rotating a token
does not modify the pair retained for rollback. An unchanged deployment whose
image lacks `--check-config` reuses its saved pair; changing its environment
or applicable configuration is refused because it cannot be preflighted.

## Configuration and rollback behavior

`DEPLOY_PATH` is the arbitrary deployment directory configured for the runner.
For example, `/opt/smarthome-ingest` may contain `compose.yaml`, `.env`, and
`config/`. Keep the complete runtime bundle in `${DEPLOY_PATH}/config/`, with
`routes.json` and its referenced schema files. Schema paths are relative to the
bundle; the shipped bundle uses `schemas/`. CD uses the host-side bundle as-is;
it does not merge files from the checked-in repository `config/` or fall back
to that directory. The host bundle must be readable by the deployment runner.

The deployment runner stores release pairs under
`${DEPLOY_PATH}/.deploy-state/<source-SHA>-<image-digest>-<config-environment-hash>/`.
Each new pair contains the resolved Compose configuration from the selected
source, a copy of the exact host-side route/schema bundle mounted read-only,
and the environment file. Resolving Compose freezes effective settings,
including values inherited from the runner, for preflight and rollback. The
candidate image's existing `--check-config` validates a new pair before
activation. Config or effective environment changes with the same image create
a separate pair; workflow-managed image and config-path entries are excluded
from the environment-file hash. Previously captured baseline and config-only
snapshot names remain usable for rollback, but snapshots with an original
Compose file can still inherit runner environment variables. On the first
capture, CD checks the running container's Compose hash against the host Compose
file and `.env`, then freezes their effective settings; keep them matched until
the baseline is captured. Both the resolved Compose file and environment file
can contain credentials; the workflow keeps the state directory mode `700` and
new snapshot files mode `600`. Compose gives runner environment variables
precedence over values in `.env`.
`.deploy-state/` is ignored by Git, is not uploaded as an artifact, and is not
automatically pruned. Protect the deployment host and back up this state
directory according to the operator's secret-retention policy.

After configuration preflight, CD activates the candidate pair, recreates only
`ingest`, and waits up to three minutes for `/readyz`. If readiness fails, the
workflow restores the previous image, Compose file, configuration, and
environment file, then verifies readiness again. It never removes WAL or
quarantine volumes. Failed deployment logs are bounded and retained as a
short-lived Actions artifact; environment or config snapshots are never
included.

Rollback is supported only when the selected binary can read the existing WAL
and quarantine journal formats; CD does not migrate journal data. The current
WAL stores length-prefixed bincode `WalEvent` records containing already-encoded
sink payloads (see [WAL and reliability](wal-and-reliability.md)). Preserve both
journal volumes across recreate and rollback; never delete them to make a
downgrade start.

Keep the stable MQTT client identity, persistent broker session, and
subscription manifest in `WAL_DIR` during rollback. Deployment does not clear
the broker session or remove the manifest. Route changes reconcile known
filters, but already queued or in-flight messages for removed routes may still
arrive and be quarantined; follow the [route migration procedure](operations.md#route-changes-and-subscription-migration).
Broker expiry and queue limits bound what survives an outage; see the
[broker deployment prerequisites](../config/examples/deployment/README.md).

The image's `/healthz` is a liveness check; successful deployment requires
`/readyz`, which verifies durable intake readiness rather than successful
InfluxDB delivery.

## First production release

Automation does not replace the first-production gate. Maintainers must first
accept the broker persistence/expiry assumptions, outage and target-hardware
evidence, and storage budgets recorded by Epic #19 and issue #38. A successful
CI/CD run alone is not that evidence and does not authorize a production
deployment.
