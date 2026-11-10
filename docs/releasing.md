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
   and runs `--check-config` against the Server-owned runtime route/schema directory
   before replacing the running service. Repository examples do not replace operator files.

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

Selecting an image other than the running image uses its most recently activated,
preserved Compose/environment pair. CD refuses the rollback if no pair exists.
The runtime directory selection still comes from the current host `.env` (or runner
environment), and its current contents must pass preflight with the selected binary.
Selecting the running image uses its revision's Compose file and the current `.env`.
Every attempt runs preflight, including unchanged same-image redeployments and saved
releases. Environment or bundle changes create a separate recovery record.

Images without `--check-config` fail before activation. A pre-unified-route rollback
requires an explicit operator procedure with its saved legacy Compose/environment
pair; it cannot pass the current automated preflight gate. Never mix the old topic
variables with a unified-route image or clear the persistent MQTT session to make a
rollback start.

## Configuration and rollback behavior

`DEPLOY_PATH` is the standalone ingest deployment directory, for example
`/opt/smarthome-stack/smarthome-ingest`, containing this repository's `compose.yaml` and an
untracked `.env` with runtime connection settings and secrets.

Set `INGEST_CONFIG_DIR=/opt/smarthome-stack/smarthome-ingest/config` in that `.env`
for the normal Server deployment. Relative paths resolve against `DEPLOY_PATH`;
an unset value defaults to its `config/` directory. Compose environment precedence
applies, so a runner's explicit `INGEST_CONFIG_DIR` overrides `.env`.
This stable directory and every referenced schema belong to Server configuration.
Repository `config/` files are generic examples. A JSON-only change needs validation
and a restart with the same image, as described in
[Operations](operations.md#route-changes-and-subscription-migration).

CD does not import a surrounding Server Compose stack. It honors the selected
runtime directory and mounts it read-only at `/app/config`, with
`INGEST_ROUTES_FILE=/app/config/routes.json`. It never overwrites a nonempty directory.
Missing directories, absent/empty `routes.json`, symbolic links, and bundles that
fail the candidate binary's validation stop deployment before activation. A fresh
deployment must supply its own complete bundle. V2 includes every listed family file and
its schemas. Snapshot copying and hashing recurse through the whole directory; candidate
preflight follows the manifest references. Keep the directory readable by the
non-root container user and do not edit it during deployment or preflight/restart.

The deployment runner stores release pairs under
`${DEPLOY_PATH}/.deploy-state/<source-SHA>-<image-digest>-<config-environment-hash>/`.
Each new pair contains the resolved Compose configuration from the selected
source, a recovery copy of the operator bundle, and the environment file. The
resolved Compose file points to the stable runtime directory, not the recovery copy.
Resolving Compose freezes effective settings, including values inherited from the
runner, for preflight and rollback. The candidate image's existing `--check-config`
always validates the actual runtime mount before activation. Snapshots with an
original Compose file can still inherit runner environment variables. On the first
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
workflow restores the previous image, Compose/environment pair and previous mount
selection, then verifies readiness again. It does not copy saved JSON over current
operator files. It never removes WAL or
quarantine volumes. Failed deployment logs are bounded and retained as a
short-lived Actions artifact; environment or config snapshots are never
included.

Automatic readiness rollback reuses the previous mount's current files. If those
files now require the newer binary, the previous image may also fail to start.
Preflight the intended rollback image against the bundle before rollout, or prepare
a separate, explicitly selected compatible recovery bundle; CD never restores old
JSON over operator edits to make rollback pass.

Configuration restoration is an explicit operator action: preserve the current
directory first, copy a chosen saved bundle to a separate runtime directory, select
it with `INGEST_CONFIG_DIR`, and validate it with the desired image before activation.
Do not select `.deploy-state/` itself as a runtime directory; it is reserved for
recovery records. If current operator configuration is incompatible with an older
image, CD fails safely rather than silently restoring that image's old JSON.

## Migrate an existing snapshot mount

Older CD wrote `.deploy-state/<release-key>/config` into `.env`. On the first new
deployment, CD recognizes that managed path and selects `${DEPLOY_PATH}/config`.
Alternatively, set `INGEST_CONFIG_DIR` to another stable Server-owned directory.
Create the destination directory if missing. When it is completely empty (including
hidden files), CD seeds it from the running container's actual `/app/config` mount,
then validates the destination with the candidate binary before switching mounts.
An existing nonempty destination is preserved and validated as-is; it is never
merged with the active bundle or repository examples. With no active bundle to seed,
an empty directory fails clearly. Validation failure leaves the active instance and
host `.env` unchanged; any seeded files remain available for inspection.

After success, `.env` records the stable directory. Verify the container's
`/app/config` mount source with `docker inspect ingest` before editing JSON there.
Recovery copies remain under `.deploy-state/` but are no longer the normal mount.

## Rollback compatibility

V1 inline bundles and v2 manifests are supported by the current binary. Earlier binaries
cannot read v2. Keep a complete v1 configuration directory paired with an older image
before opting into v2, and explicitly select that compatible directory for rollback.
Image rollback alone cannot restore format compatibility. See the
[manual bundle migration](messages-and-routing.md#migrate-v1-to-v2); CD preserves operator
files and does not convert formats or restore configuration snapshots automatically.

Rollback is supported only when the selected binary can read the existing WAL
and quarantine journal formats; CD does not migrate journal data. The current
WAL stores length-prefixed bincode `WalEvent` records containing already-encoded
sink payloads (see [WAL and reliability](wal-and-reliability.md)). Preserve both
journal volumes across recreate and rollback; never delete them to make a
downgrade start.

Targeted output frames require the new binary. Drain legacy output with the old
binary before upgrade; a binary downgrade needs a fully drained, preserved journal
and a separate fresh output WAL. See the exact
[sink-target migration policy](wal-and-reliability.md#sink-target-migration) before
selecting an older image. CD does not perform this migration.

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
