# CI Build Cache

This document describes how build caching works in CI, known issues, and how to diagnose problems.

## How It Works

The "Build and Test" workflow uses [ccache](https://ccache.dev/) to cache compiled object files, backed by [GitHub Actions cache](https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows) for persistence across runs.

**Components:**
- **ccache**: Caches compiled C/C++ object files locally. Keyed on preprocessed source content + compiler flags. Compression is enabled.
- **hendrikmuhs/ccache-action**: Saves/restores the ccache directory to/from GitHub Actions cache between runs.
- **GitHub Actions cache**: Remote storage with a configurable per-repository quota (default 10GB, can be increased via org settings).

**Cache key scheme** (defined in `.github/actions/setup-villagesql-build/action.yml`):
- Save key: `ccache-<OS>-<prefix>-<commit-sha>` (unique per commit)
- Restore key prefix: `ccache-<OS>-<prefix>` (matches most recently saved entry)

**Branch isolation** (GitHub Actions rule):
- `main` branch builds can only restore caches saved by other `main` builds
- Feature branch builds can restore from their own branch OR from `main`
- Feature branches cannot see other feature branches' caches

**Stale entry eviction:**
After each build, the workflow runs `ccache --evict-older-than 3600s` to prune cache entries not accessed during the build. This prevents the saved cache from bloating with stale objects from old branches/commits. Without this step, caches grow over time as different branches accumulate objects, eventually hitting the max-size and triggering LRU eviction during builds.

## Cache Sizing

The ccache `max-size` is set to 5GB. This is chosen to fit two cache entries within the GitHub Actions 10GB default quota (since ccache compression means GH entries are roughly the same size as the on-disk ccache directory).

Only "Build and Test" and "Nightly" workflows have caching enabled. Other infrequent workflows (full-test-suite, valgrind, release) have caching disabled to avoid polluting the quota (see PR #120).

## Performance Tiers

Based on analysis (April 2026), builds fall into three tiers:

| Tier | ccache hit% | Build time | Cause |
|------|------------|------------|-------|
| Full cache hit | 99%+ | ~5 min | Cache restored, all entries valid |
| Partial miss | ~82% | ~10-13 min | Cache restored, but many entries stale (code diverged from cached commit) |
| Total miss | ~1.5% | ~20-24 min | "No cache found" - GH cache entry was evicted |

Test times are consistently 2-3 minutes regardless of cache state.

The partial miss tier occurs when the restored cache was saved from a commit that differs significantly from the current build. The miss count depends on how many files changed (directly or via header dependencies) between the cached commit and the current one. The `--evict-older-than` step ensures these stale entries don't persist into the saved cache.

## Known Issues

### GitHub Actions cache quota eviction

**Symptom**: "No cache found" in logs, ~1.5% hit rate, 20+ minute builds.

**Root cause**: Each build saves a new cache entry keyed by commit SHA. With active development, the quota fills up and older entries are evicted. If all recent `main` entries get evicted, the next `main` build has no cache.

**Mitigation**: The stale entry eviction step keeps cache entries lean. Only frequently-used workflows have caching enabled. The GitHub Actions cache quota can be increased beyond 10GB via org/enterprise settings if needed.

### Queue wait on self-hosted runners

**Symptom**: Very long total run times (30+ minutes to hours) but normal build/test times.

**Root cause**: Only one self-hosted runner is available. When multiple builds queue up, later ones wait.

**Diagnosis**: Compare `createdAt` (when run was triggered) vs job `startedAt` (when runner picked it up).

## Diagnosing Cache Issues

### Quick check via logs

Look for these lines in a build's log output:

```
# Good: cache was restored
Restored from cache key "ccache-Linux-build-<sha>-".

# Bad: no cache available
No cache found.
```

### ccache statistics

The "Show ccache statistics" step prints hit/miss rates and cache size:

```
# Good
Hits:            4536 / 4557 (99.54%)
Misses:            21 / 4557 ( 0.46%)
Cache size (GB):  1.4 /  5.0 (28.00%)
Cleanups:           0

# Partial miss (code diverged from cached commit)
Hits:            3761 / 4557 (82.52%)
Misses:           796 / 4557 (17.48%)

# Total miss (no cache restored)
Hits:              72 / 4557 ( 1.58%)
Misses:          4485 / 4557 (98.42%)
```

Key things to check:
- **Cleanups > 0**: The cache is hitting max-size and evicting entries during the build. Consider increasing `max-size`.
- **Cache size near max**: Stale entries may be accumulating. Check that the `--evict-older-than` step is running.

### Listing cache entries

```bash
gh cache list -R villagesql/villagesql-server
```

Check how many entries exist and their sizes. If entries are large (approaching max-size), the eviction step may not be working.

## History

- **PR #120** (March 2026): Increased ccache from 2GB to 4GB. Disabled caching for infrequent workflows to reduce cache quota pressure.
- **April 2026**: Increased ccache to 5GB. Added `ccache --evict-older-than 3600s` after builds to prune stale entries before saving.
