# Merging an Oracle Upstream Release into VillageSQL

VillageSQL is a fork of MySQL 8.4 carrying the VillageSQL Extension Framework (VEF).
Bringing in a new Oracle release is a **3-way merge**: a common upstream base, *ours*
(VillageSQL `main`), and *theirs* (the new `mysql-8.4.x` tag).

Treat it as production-database surgery. We are responsible for our users' users' data.

**Related files in this directory:**

- [`recurring-conflicts.md`](recurring-conflicts.md) — the files that conflict on
  *every* merge, how we resolve each, and why. Read this before resolving anything.
- [`post-merge-checks.md`](post-merge-checks.md) — things that never conflict and so
  never announce themselves: case sensitivity, I_S schema checksums. Nothing in the
  merge will make you look at these; you have to remember to.
- `recurring-conflicts-<target>.md` — what is specific to one merge target, when a
  target has specifics worth recording. Currently
  [`recurring-conflicts-9.7.md`](recurring-conflicts-9.7.md), for the 9.7 port branch
  and its criss-cross merge base. Read the shared file plus the one for your target,
  and no others — nobody should have to read past entries that cannot affect them.

Per-merge working notes — inputs, scale, this-merge resolutions, build and test
results, triage — do **not** live here. They describe the state of one operation in
progress, not how to do the work, and they go stale the moment the merge lands. Keep
them outside the repo in a `<version>-merge-metadata/` directory alongside your build
trees. What belongs here is only what the *next* merge needs.

**Provenance.** This playbook was written during the **mysql-8.4.11** merge
(2026-08-19) and draws on the 8.4.10 merge before it. Phases 0 and 1 are described as
executed. Later phases carry forward from prior merges and are marked where they have
not yet been re-exercised.

---

## Phase 0 — pin inputs and measure

**Pin all three points as exact commits before touching anything.** Record them in your
out-of-repo merge notes.

```bash
git fetch <upstream-remote> --tags
git merge-base main mysql-8.4.x        # the base
```

**Anchor on the tag, not the branch head.** They are usually the same commit, but the
tag is reproducible and will not move if the next release ships mid-effort.

**`main` moves under you.** Re-check `origin/main` immediately before creating the
merge, not just at the start of the effort. During the 8.4.11 merge `main` advanced 13
commits between pinning the inputs and creating the merge, and the first attempt was
built on a stale parent. Verify against the live remote:

```bash
git ls-remote origin refs/heads/main
```

**Understand the scale before resolving.** Two numbers matter, and they are different:

```bash
git rev-list --count <base>..<theirs>              # total upstream commits
git rev-list --count --no-merges <base>..<theirs>  # substantive ones
```

Expect the count to be larger than the version bump suggests. Oracle cuts a release tag
off a release branch, so mainline development between two release cuts only becomes
visible at the later tag. 8.4.10 → 8.4.11 looked like one point release and was 249
commits (151 non-merge) reaching back four months.

**Measure the conflict surface.** This is the single most useful number, and it takes
about a minute:

```bash
git diff --name-only <base> <theirs> | sort > /tmp/upstream-files.txt
git diff --name-only <base> <ours>   | sort > /tmp/vsql-files.txt
comm -12 /tmp/upstream-files.txt /tmp/vsql-files.txt
```

Only files both sides touched can conflict. For 8.4.11 that was 31 files out of 852
upstream / 3878 ours — which turned a merge that *sounded* enormous into a half-day of
resolution. Do this before estimating anything.

## Phase 1 — the mechanical merge

```bash
git merge --no-commit --no-ff <theirs>
git diff --name-only --diff-filter=U     # what actually conflicted
```

The set that actually conflicts is much smaller than the overlap — most overlapping
files merge cleanly because the two sides touched different regions. For 8.4.11, 4 of
the 31 conflicted.

**Resolve by class:**

| Class | Examples | Disposition |
|---|---|---|
| Fork identity | `LICENSE`, `CONTRIBUTING.md` | take-ours, whole file |
| Version | `MYSQL_VERSION` | see `recurring-conflicts.md` |
| Error catalogs | `share/messages_to_*.txt` | take-both, then audit for error-number collisions |
| Additive source | independent new blocks | take-both |
| Semantic | both sides changed the same thing | manual, one at a time, with a logged reason |

**Take-both is wrong for test files.** Both sides usually edited the same expected
output, so take-both produces doubled output that fails at test time — or silently
breaks a shared `.inc`. Resolve test files semantically, or plan to re-record them.

**Log every non-trivial resolution** in the merge record: file, disposition, what
happened to the VillageSQL side (preserved / dropped-and-why), and the reasoning. If a
file appears here for the second time, promote it to `recurring-conflicts.md`.

**Exit criteria:**

```bash
git diff --name-only --diff-filter=U          # must be empty
git grep -nE '^(<<<<<<<|>>>>>>>)'             # see below
```

The marker grep **never comes back empty** — there are inherited false positives
committed upstream. They are catalogued in `recurring-conflicts.md`. The general rule:
before "fixing" any marker, check whether it is present in both parents. If it is, it
is not merge residue.

## Phase 2 — build

*Carried forward from prior merges; re-exercise and correct.*

- Configure from an **empty** directory. A reused one silently keeps stale `-D` flags
  and has produced fake "broken" results.
- Build **everything** — do not cherry-pick targets. Determine the core count with
  `getconf _NPROCESSORS_ONLN` and pass it to `make -j`.
- Build **both** release and debug (`-DWITH_DEBUG=1`). Debug catches `assert`/`ut_a`
  failures release hides, and many `*_debug` tests only run there.
- Record the exact feature-skip flags used, so the suites they disable are understood
  as expected skips rather than regressions.

## Phase 3 — test wide

*Carried forward from prior merges; re-exercise and correct.*

Run the **full default suite**, not `--do-suite=village`. A VillageSQL-only run
structurally cannot see unmerged upstream `.result` files or upstream regressions,
because our subsuite never touches upstream test files. Run on the **debug** build for
assert coverage.

**The dangerous class is the clean merge, not the conflict marker.** Git forces you to
look at conflicts; the bugs are VEF code meeting a *changed* upstream signature or
default with no textual conflict at all. In 8.4.11, 27 of 31 overlapping files merged
silently. Budget verification effort *after* the merge resolves clean.

## Phase 4 — triage

*Carried forward from prior merges; re-exercise and correct.*

Classify **every** failure, with a stated why, into:

- **(a)** a clean-merge behavior/output change our code silently depended on
- **(b)** an error introduced while resolving a conflict
- **(c)** pre-existing / environmental / flaky — not a merge defect

Rules:

1. **Explain why it broke before any fix.** "This makes the test pass" is not a
   diagnosis; a masked symptom can hide a real regression.
2. **Reproduce consistently before concluding.** One pass is not "passes"; one failure
   is not "always fails". For anything non-deterministic, run it N times per build and
   report the rate.
3. **Prove merge-vs-pre-existing with parent baselines.** Build *ours* and *theirs* in
   separate worktrees with the same flags, and run the suspect test in each:
   - ours also fails ⇒ pre-existing on our side
   - theirs also fails ⇒ upstream bug, faithfully carried
   - both parents clean, only the merge fails ⇒ genuine merge regression
4. **Diff against the parent before blaming the merge.** If the failing code is
   byte-identical to a parent, the merge did not introduce it.
5. **Fix the cause everywhere.** The same upstream change usually hits sibling code
   paths no current test exercises.
6. **Cover fixes test-first.** Add the test, confirm it is red, then fix.

## Phase 5 — re-records

*Carried forward from prior merges; re-exercise and correct.*

Most merge-attributable failures are `.result` files needing a re-record. Before
recording, confirm the diff is benign and not a real behavior change you would be
baking in.

```bash
./mysql-test/mysql-test-run.pl --record <test>
```

Then **verify on a clean non-record run.**

Two classes of re-record recur and are written up separately, because nothing in the
merge will stop you and make you look at them — see
[`post-merge-checks.md`](post-merge-checks.md): case sensitivity (`lctn`) and the I_S
schema checksums.

## Phase 6 — landing

Land only once the merge is verified, never before.

**For agents: do not push. Only a human must push.** Every step in this phase is a
human action. An agent prepares the branch and hands over the commands.

**Authorship.** The merge commit is authored by the person doing the merge. Do not
copy an author from a previous merge.

**Redo the merge immediately before pushing.** `main` moves while you verify. The merge
commit's first parent must be the current tip of `main`, or the push is not a
fast-forward and will be rejected. Re-run Phase 1 against the current tip and re-check
the conflict set — during the 8.4.11 merge `main` advanced 13 commits between pinning
the inputs and creating the merge.

**A merge commit is deliberately non-linear, and `main` forbids that.** GitHub has a
ruleset on `main` requiring linear history. An upstream merge is one of the rare
commits where we *want* the two-parent shape — it is what records which upstream tag we
merged, and squashing it away destroys that. So landing requires temporarily bypassing
the ruleset:

1. **Add the person doing the merge to the bypass list** for the `main` linear-history
   ruleset.
2. **Push the merge directly to `main` from the command line**, skipping the pull
   request. The GitHub UI squashes, which flattens the merge and defeats the purpose.

   **Generate the push command with a compare-and-swap**, so it fails rather than
   lands if `main` moved since you merged:

   ```bash
   git push --force-with-lease=main:<sha main was at when you re-merged> \
     origin <merge-commit>:main
   ```

   Not the remote's current sha — `--force-with-lease` passes whenever the remote
   matches the value you give it, so that degrades to a plain `--force`.
3. **Remove them from the bypass list** (do this even if you drop the merge, within an
   hour).
4. **Close the pull request** with a note explaining that the branch was landed
   directly as a merge commit and why, so the PR is not left looking abandoned and the
   next person finds the reasoning.

## Things you notice that are not the merge

A merge drags you through parts of the tree nobody has looked at in months, so you
*will* find problems. Almost none of them are merge work.

**Do them separately.** A merge commit that also fixes an unrelated bug is harder to
review, harder to revert, and blurs the line between "upstream changed this" and "we
changed this". The merge branch should carry the merge, and nothing that would still
need doing if the merge were abandoned.

**But write them down before you lose them.** These findings are cheap to make and
expensive to re-derive — you had to be deep in the tree to see them at all, and the next
person will not be. Anything you defer goes somewhere durable: an issue, a note in your
working file, a message to whoever owns it. "I noticed and moved on" means it is gone.

The test: *would this still need doing if the merge were abandoned?* If yes, it is not
merge work.

### Worked examples, from the 8.4.11 merge

**villint stamps VillageSQL copyright onto vendored code.** The merge brought in new
`extra/libcno/` and `extra/libfido2/` files; villint added "VillageSQL Contributors" to
them and hard-errored on libfido2's non-standard header. Adding our copyright to
third-party source is a licensing error, so it must be suppressed, never accepted.

- *In the merge:* `villint-ignore:` trailers on the merge commit, listing `extra/*` and
  the new upstream files. Narrow, scoped to this merge, correct.
- *Not in the merge:* `extra/` is third-party **by definition** and arguably should
  never be copyright-linted at all. That is a change to `scripts/villint.sh`, it would
  fix the problem for every future merge, and it has now been rediscovered at least
  twice — the 8.4.10 merge hit it too and left nothing behind. Exactly the shape of
  thing that should be filed rather than folded in.

**`Docs/LICENSE.historical` is stale.** Frozen at `MySQL 8.4.5 Community`, March 2025,
and never refreshed since. Noticed while resolving `LICENSE`; not caused by any merge,
and refreshing it is its own reviewed change. See `recurring-conflicts.md`.

**Only `main`'s VillageSQL suite is routinely tested.** The nightly runs
`--do-suite=village` and the big tests, and nothing else — no `--all-suites` step. So
everything outside our own suite is exercised only by manually dispatched runs on
branches. That is how a stale checksum survived from July to August unnoticed. It also
means **a green `main` is not a baseline** for anything outside the village suite, which
matters when you are triaging.

## Definition of done

- No unresolved paths; no conflict markers beyond the catalogued false positives.
- Clean build, release and debug.
- Full default suite run; every failure triaged into (a)/(b)/(c) with a stated why;
  zero unexplained failures.
- Confirmed merge regressions fixed, applied to sibling code paths, covered test-first.
- Anything that conflicted for a second time has been promoted to
  `recurring-conflicts.md`, and any newly learned rule has been promoted into this
  playbook. Findings that stay only in your working notes are lost when the merge lands.
- Landed as a merge commit on `main`, the pull request closed with a note explaining
  how, and **the bypass removed from the `main` ruleset**.
- Everything you noticed but deferred is filed somewhere durable, not left in the
  merge's working notes.
