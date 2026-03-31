---
name: prepare-release
description: |
  Prepare a new release of squirreling: run checks, update changelog, bump version, and commit.
---

# Prepare Release

Prepare a new version release of the squirreling package.

## Steps

1. **Determine the new version number.** Read `package.json` and the commits since the last release. Infer the bump type from the commits:
   - Breaking changes or major new features → minor bump (e.g. 0.11.1 → 0.12.0)
   - Bug fixes, small improvements → patch bump (e.g. 0.11.1 → 0.11.2)
   - Ask the user to confirm if there is ANY uncertainty about the bump type.

2. **Run all checks.** All three must pass before proceeding:
   ```bash
   npm test
   npm run lint
   npx tsc
   ```

3. **Update CHANGELOG.md.** Add a new section at the top (after the `# Squirreling Changelog` heading) summarizing the changes since the last release. Follow the existing format:
   ```
   ## [X.Y.Z]
    - Description of change 1
    - Description of change 2
   ```
   Use the commit messages and diffs to write concise, user-facing descriptions. Focus on what changed from a user's perspective, not internal details.

4. **Bump version in package.json.** Update the `"version"` field to the new version.

5. **Commit the changes.** Stage `CHANGELOG.md` and `package.json`, then commit with message `Publish vX.Y.Z`. NEVER sign commits as Claude or any AI co-authors.

6. **Tag the release.** Create a lightweight git tag: `git tag vX.Y.Z`.

7. **Remind the user** to run `npm publish` and `git push && git push --tags` when ready.
