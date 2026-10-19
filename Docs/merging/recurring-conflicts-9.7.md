# Recurring Conflicts — 9.7 port branch

Companion to [`recurring-conflicts.md`](recurring-conflicts.md). That file covers what
conflicts on **any** Oracle upstream merge, because it arises from VillageSQL's
divergence from Oracle rather than from which Oracle branch you are merging. This file
covers what is specific to the **9.7 port** — a branch carrying an older VillageSQL base
with a newer upstream merged into it.

Read both. Nothing here fires on a straight 8.4 merge.

---

# Criss-cross merge bases

**This is the dominant fact about merging into a port branch, and it will mislead you
before you notice it.**

**Symptom:** a merge reports dozens of conflicts in files nobody on our side has ever
edited — generated man pages, bundled licences, vendored artifacts.

**Cause:** more than one merge base. A port branch shares *two* ancestors with upstream,
not one. Git synthesises a virtual base by merging the candidates, and files that
differ between those two candidates come out conflicted even though neither real side
changed them.

**Check for it before estimating anything.** More than one head means criss-cross:

```bash
git merge-base --all <ours> <theirs>
```

A conflict that presents three sides rather than two is the same signal.

**Resolution:** confirm our side is unmodified against the base our branch actually
derives from, then take theirs in bulk:

```bash
git diff <base-our-branch-derives-from> <ours> -- <path>   # empty ⇒ artifact
```

**Check the whole directory, not just the conflicted files.** That is what makes a bulk
`restore` safe — a file that conflicted tells you nothing about its neighbours.

**Worked instance — 9.7.0 → 9.7.2:** 68 conflicted files, of which **64 were
artifacts** — 63 `man/*` pages plus `router/LICENSE.router`, every one byte-identical to
`mysql-9.7.0` on our side. Only 4 were genuine, and all 4 were fork-identity or version
files already covered by the shared file. Zero conflicts in `sql/`, `storage/`, or
`mysql-test/`.

The conflict-surface estimate does not predict this: the overlap measurement said 45
files, and 68 conflicted, because the virtual base manufactures conflicts in files
neither side touched.

# `router/LICENSE.router`

**Usually an artifact, not a genuine conflict.** Confirm our side is unmodified against
the base, then take theirs. Apply the shared file's new-third-party-text check if the
diff is large rather than a version-string edit.

# `.github/PULL_REQUEST_TEMPLATE.md`

**Conflicts because** both projects maintain a PR template at the same path.

**Resolution: take ours, whole file.**

Ours covers CLA, `CONTRIBUTING.md`, tests, and upstream-compatibility. Oracle's covers
the OCA, `scripts/ci/` invocations, an AI-assistance disclosure, and an "areas touched"
section for their auto-routing. Their CI paths and routing do not apply to us.

Has not yet conflicted on a straight 8.4 merge; if it does, promote this entry into the
shared file.

# Instances of shared rules, seen here

These illustrate rules that live in the shared file. Recorded here so the shared file
does not accumulate per-branch narration.

**`LICENSE` — the case that proves the diff-check matters.** 9.7.0 → 9.7.2 was 167
insertions / 5 deletions: beyond the version strings it added an **OpenTelemetry C++**
notice (`opentelemetry-cpp`, Apache-2.0) and a "4th Party Dependencies" section for
`opentelemetry-proto`. Note what it was *not*: zero opentelemetry files changed between
those releases, and `cmake/opentelemetry-cpp.cmake` already pinned
`opentelemetry-cpp-1.23.0` at 9.7.0 with sources in `extra/`. Oracle was correcting an
omission in their own licence book for something already shipping.

**Outstanding on this branch:** we bundle `opentelemetry-cpp` under Apache-2.0, and
neither our `LICENSE` nor `Docs/LICENSE.historical` carries the notice. Taking ours on
`LICENSE` is correct and does **not** fix this. The notice needs to reach
`LICENSE.historical`, which is frozen at `MySQL 8.4.5 Community`, March 2025.

**`CONTRIBUTING.md`.** 9.7.2's rewrite added an AI-assistance disclosure section — an
example of upstream process ideas worth considering in our own words, which is a
deliberate choice rather than a merge resolution.

**`MYSQL_VERSION`.** This branch carries the comment explaining why
`MYSQL_VERSION_EXTRA` is intentionally omitted; the shared file reuses that wording.
Resolution here is the same: take ours, hand-bump the patch level.
