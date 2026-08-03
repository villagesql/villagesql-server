# Contributing Guide

## Welcome to VillageSQL Server for MySQL!

"It takes a village"

Open source software only succeeds with a strong community. We are excited you are here and considering a contribution to VillageSQL. There is a simple process to submit changes.

As a general rule, any change that breaks compatibility with upstream MySQL will not be accepted. Similarly, any change that impacts the long-term maintainability of the VillageSQL fork will also be rejected. If you want to make changes that introduces non-compatible changes, please consider authoring an extension instead.

## Where Does My Contribution Belong?

**Building new functionality** (custom functions, custom types, integrations, exporters, etc.):
Build a VEF extension in your own repository. Clone or fork
[vsql-extension-template](https://github.com/villagesql/vsql-extension-template) to get started.
See [Extensions or Plugins and Components](https://villagesql.com/docs/mysql-8.4/0.0.3/extensions-or-plugins)
for guidance on when to use VEF versus MySQL's native plugin and component interfaces.

**Improving VEF itself** (new hook types, SDK capabilities):
First check if there's an existing Issue you can upvote. If not, [submit a new Issue](../../issues/new/choose). You can also submit a PR to this repo. Please reference the applicable issue in your PR.

**Server bug fixes and MySQL compatibility**:
File an issue and follow the process below.

For VEF improvements and bug fixes, please follow this process:

1. File a Github [issue](./issues) for any improvement or feature request. Use the appropriate prefix for your issue type: `[Server]:` for server changes, `[Bug]:` for bugs, `[VEF Hook]:` for extension framework work, `[Extension]:` for ideas of extensions you would like to see built. Browse the [project board](https://github.com/orgs/villagesql/projects/1) to see what's already planned or in progress before filing.
2. All requests will be reviewed weekly by VillageSQL
3. Once a Github [issue](./issues) and solution has been agreed to, submit a pull request including signing the CLA

### Submitting a Pull Request

External contributors do not have write access to this repository, so changes
are shared through a fork:

1. **Fork** `villagesql/villagesql-server` to your own GitHub account (use the **Fork** button on the repository page).
2. **Clone your fork** and create a branch for your change:
   ```bash
   git clone https://github.com/<your-username>/villagesql-server.git
   cd villagesql-server
   git checkout -b my-change
   ```
3. Commit your work, then **push the branch to your fork**:
   ```bash
   git push origin my-change
   ```
4. **Open a pull request** from your fork's branch against `villagesql/villagesql-server`'s `main` branch. Reference the agreed-upon issue in the PR description (e.g. `Closes #NNN`).
5. **Sign the CLA.** On your first pull request, the CLA Assistant bot comments with a link to the CLA and the instructions to sign; follow them to unblock the PR.

We look forward to hearing from you.

## Issue Tracking

We use GitHub Issues to track all planned, in-progress, and ultimately, shipped work. We treat GitHub Issues as the source of truth for VillageSQL Server. Issues map to discrete pieces of work — features, bugs, internal tasks, etc. As an open source project, we do this in order to be transparent in what we are currently working on and the scope of our vision for VillageSQL Server.

We use a GitHub Project board as the presentation layer: [https://github.com/orgs/villagesql/projects/1](https://github.com/orgs/villagesql/projects/1). The board organizes issues by Milestone and Status. When an issue is first submitted it has no status — that's the signal for it to be triaged. After triage, an issue either gets assigned to a milestone and set to Planned, or it is placed on the backlog (with no milestone set). Issues in a milestone progress through Planned, In Progress, and Shipped. Issues that have been triaged but not yet assigned to a milestone carry Backlog status. Milestone and Status live on the Issue itself, so the board and the issue page always agree.

Status moves forward in two ways. PRs drive most transitions automatically — adding `Closes #NNN` to a PR description closes the issue and moves it to Shipped when the PR merges. For earlier stages, status is updated directly on the issue page: when work is committed to a milestone it moves to Planned, and to In Progress when someone picks it up. If a PR is related to an issue but doesn't fully resolve it, referencing the issue number without `Closes` connects the history and keeps the issue open.

Milestones represent planned scope for a release. An issue without a milestone is either unplanned or declined. If something is explicitly won't-fix or a duplicate, it should get a `wontfix` or `duplicate` label, the milestone is removed, and is closed. That keeps milestone views in the Project clean without needing a separate status for declined work.
