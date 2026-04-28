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
Submit a PR to this repo. If a hook you need isn't supported yet, file an issue or upvote an existing one.

**Server bug fixes and MySQL compatibility**:
File an issue and follow the process below.

To contribute to VillageSQL, please follow this process:

1. File a Github [issue](./issues) for any improvement or feature request. Use the appropriate prefix for your issue type: `[Server]:` for server changes, `[Bug]:` for bugs, `[VEF Hook]:` for extension framework work, `[Extension]:` for ideas of extensions you would like to see built. Browse the [project board](https://github.com/orgs/villagesql/projects/1) to see what's already planned or in progress before filing.
2. All requests will be reviewed weekly by the Village council (i.e. committers)
3. Once a Github [issue](./issues) and solution has been agreed to, submit a pull request including signing the CLA

We look forward to hearing from you.

## Issue Tracking

We use GitHub Issues to track all planned, in-progress, and ultimately, shipped work. We treat GitHub Issues as the source of truth for VillageSQL Server. Issues map to discrete pieces of work — features, bugs, internal tasks, etc. As an open source project, we do this in order to be transparent in what we are currently working on and the scope of our vision for VillageSQL Server.

We use a GitHub Project board as the presentation layer: [https://github.com/orgs/villagesql/projects/1](https://github.com/orgs/villagesql/projects/1). The board organizes Issues by Milestone and status: Backlog, Planned, In Progress, and Shipped. Milestone and Status live on the Issue itself, so the board and the issue page always agree.

Status moves forward in two ways. PRs drive most transitions automatically — adding `Closes #NNN` to a PR description closes the issue and moves it to Shipped when the PR merges. For earlier stages, status is updated directly on the issue page: when work is committed to a milestone it moves to Planned, and to In Progress when someone picks it up. These changes can be made directly to the Issue or by moving cards on the Project view. If a PR is related to an issue but doesn't fully resolve it, referencing the issue number without `Closes` connects the history and keeps the issue open.

Milestones represent planned scope for a release. An issue without a milestone is either unplanned or declined. If something is explicitly won't-fix or a duplicate, it should get a `wontfix` or `duplicate` label, the milestone is removed, and is closed. That keeps milestone views in the Project clean without needing a separate status for declined work.
