# Releasing and deploying

## Validation gates

Pull requests and pushes to `master` run formatting, clippy, locked tests, the
isolated EMQX/InfluxDB integration suite, and Compose validation. The CD workflow
calls the same CI workflow for the exact source revision it will build or
redeploy; a successful run for another commit is not a release gate.

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
   not a mutable tag. It stages the route/schema bundle and Compose file from the
   same source revision, then runs `--check-config` against the exact mounted
   bundle before replacing the running service.

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

The source revision selects a matching Compose file, route/schema bundle, and
host-local environment snapshot. This is required for rollback across the
unified-route cutover: a pre-cutover image needs its matching legacy environment
variables, while a post-cutover image rejects them. A revision without a
preserved compatible environment snapshot cannot be deployed as a rollback.

## Configuration and rollback behavior

The deployment runner stores release pairs under
`$DEPLOY_PATH/.deploy-state/<source-SHA>-<image-digest>/`. Each pair contains
the versioned Compose file, read-only route/schema bundle, and environment
snapshot. The environment file may contain credentials; the workflow keeps the
state directory mode `700` and environment files mode `600`. `.deploy-state/`
is ignored by Git, is not uploaded as an artifact, and is not automatically
pruned. Protect the deployment host and back up this state directory according
to the operator's secret-retention policy.

CD uses only the checked-in `config/` bundle from the selected source revision.
Host-local configuration overlays are not merged or validated and are not a
supported deployment input; include deployment routes and schemas in the
versioned bundle.

After configuration preflight, CD activates the candidate pair, recreates only
`ingest`, and waits up to three minutes for `/readyz`. If readiness fails, the
workflow restores the previous image, Compose file, configuration, and
environment file, then verifies readiness again. It never removes WAL or
quarantine volumes. Failed deployment logs are bounded and retained as a
short-lived Actions artifact; environment snapshots are never included.

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
