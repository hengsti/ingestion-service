# Releasing & Deploying the Ingestion Service

## CI - runs automatically

Every push and pull request to `master` runs formatting, clippy, and tests.
No action required.

## CD - runs only when you trigger it

### Release a new version (recommended)

1. Ensure master is in the desired state and CI is green.
2. Tag the commit with a SemVer version:
   ```bash
   git tag v1.2.3
   git push origin v1.2.3
   ```
3. The `cd.yaml` workflow starts automatically:
   - Builds and pushes the Docker image tagged `:v1.2.3` and `:latest`
   - Deploys the new image to the self-hosted runner

### Redeploy an existing image (rollback or re-apply)

Use this to roll back or re-apply a previously built image without triggering a new build.

1. Go to **Actions -> CD / ingestion-service -> Run workflow**
2. Enter the existing image tag (e.g. `v1.2.3`) in the `image_tag` field
3. Click **Run workflow** - the deploy job runs directly, build is skipped

### Manual build + deploy (ad-hoc, no tag)

1. Go to **Actions -> CD / ingestion-service -> Run workflow**
2. Leave `image_tag` empty
3. Click **Run workflow** - builds a `sha-<hex>` tagged image and deploys it

## Image tags in GHCR

| Tag | Meaning |
|---|---|
| `:latest` | Most recently deployed release |
| `:v1.2.3` | Specific release, pinned forever |
| `:sha-<12hex>` | Ad-hoc build from manual dispatch |
