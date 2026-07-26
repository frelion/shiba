# Releasing Shiba

Shiba releases are GitHub Releases backed by version tags.

## Maintainer checklist

1. Update the version in `Cargo.toml` and `shiba.control`.
2. Run `./scripts/test-all.sh` on PostgreSQL 17.
3. Update the user-facing changes in the release notes.
4. Commit the version change and create an annotated tag:

   ```bash
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin main --follow-tags
   ```

5. The `release.yml` workflow installs PostgreSQL 17 development files,
   packages the extension with `cargo pgrx package`, creates a tarball and
   SHA-256 checksum, and attaches both to the GitHub Release.

## Installing a release artifact

Download the PostgreSQL 17 artifact and checksum from the release page, verify
it, then unpack it into the target PostgreSQL installation using the paths in
the package:

```bash
sha256sum -c shiba-v0.1.0-postgresql17.tar.gz.sha256
tar -xzf shiba-v0.1.0-postgresql17.tar.gz -C /tmp/shiba-package
```

The package is intentionally PostgreSQL-major-version-specific. A future
PostgreSQL major version should receive its own CI and release artifact.
