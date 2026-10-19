# Contributing Guide

## Welcome to VillageSQL Server for MySQL!

"It takes a village"

Open source software only succeeds with a strong community. We are excited you are here and considering a contribution to VillageSQL. There is a simple process to submit changes.

As a general rule, any change that breaks compatibility with upstream MySQL will not be accepted. Similarly, any change that impacts the long-term maintainability of the VillageSQL fork will also be rejected. If you want to make changes that introduce non-compatible changes, please consider authoring an extension instead.

## Getting Started

- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
    - [Create a GitHub account](#create-a-github-account)
    - [Development Environment](#development-environment)
    - [Code of Conduct](#code-of-conduct)
  - [Where Does My Contribution Belong?](#where-does-my-contribution-belong)
  - [Issue Tracking](#issue-tracking)
  - [Community](#community)
    - [Communication](#communication)
  - [GitHub Workflow](github-workflow.md)

## Prerequisites

Before you contribute to VillageSQL, please ensure you have read the below prerequisites. Some are technical in nature, and others establish community standards and guidelines. Everything is important for ensuring a smooth first contribution and a healthy community.

### Create a GitHub Account

Contributing to VillageSQL requires you to [sign up](http://github.com/signup) for a GitHub account.

### Code of Conduct

We have a [Code of Conduct](../CODE_OF_CONDUCT.md), which all contributors and maintainers must abide by.

### AI Use and Disclosure Policy

You are welcome to use AI when drafting your PR. If you do, you must disclose that in the PR description. For example, including "This PR was written with the assistance of AI," is acceptable. All AI-generated code is still your responsibility to understand, and you must be prepared to answer questions about it.

Your PR description itself, commit messages, Issue descriptions, and all replies to reviewers must be written by you, without the assistance of AI. We want to talk to you, not your LLM. We do not allow the use of AI code review assistants such as Copilot.

### Where Does My Contribution Belong?

**Building new functionality** (custom functions, custom types, integrations, exporters, etc.):
Build a VillageSQL Extension Framework (VEF) extension in your own repository. Fork
[vsql-extension-template](https://github.com/villagesql/vsql-extension-template) to get started.

See [Extensions or Plugins and Components](https://villagesql.com/docs/mysql-8.4/0.0.5/extensions-or-plugins#extensions-or-plugins-and-components)
for guidance on when to use VEF versus MySQL's native plugin and component interfaces.

We also provide an [extention builder skill](https://github.com/villagesql/villagesql-skills) to help you.

**Improving VEF itself** (new hook types, SDK capabilities):
First check if there's an [existing Issue](../../issues) you can upvote. If not, [submit a new Issue](../../issues/new/choose). You can also submit a PR to this repo. Please reference the applicable issue in your PR.

**Server bug fixes and MySQL compatibility**:
File an issue and follow the process below.

For VEF improvements and bug fixes, please follow this process:

1. First check if there's an existing Issue you can upvote.
2. File a Github [issue](../../issues) for any improvement or feature request. Browse the [project board](https://github.com/orgs/villagesql/projects/1/views/4) to see what's already planned or in progress before filing.
3. All requests will be reviewed weekly by VillageSQL
4. Once a Github [issue](../../issues) and solution has been agreed to, submit a pull request including signing the CLA

We look forward to hearing from you.

### Issue Tracking

We use GitHub Issues to track all planned, in-progress, and ultimately, shipped work. We treat GitHub Issues as the source of truth for VillageSQL Server. Issues map to discrete pieces of work — features, bugs, internal tasks, etc. As an open source project, we do this in order to be transparent in what we are currently working on and the scope of our vision for VillageSQL Server.

We use a [GitHub Project board](https://github.com/orgs/villagesql/projects/1) as the presentation layer. The board organizes issues by Milestone and Status. When an issue is first submitted it has no status — that's the signal for it to be triaged. After triage, an issue either gets assigned to a milestone and set to Planned, or it is placed on the backlog (with no milestone set). Issues in a milestone progress through Planned, In Progress, and Shipped. Issues that have been triaged but not yet assigned to a milestone carry Backlog status. Milestone and Status live on the Issue itself, so the board and the issue page always agree.

Status moves forward in two ways. PRs drive most transitions automatically — adding `Closes #NNN` to a PR description closes the issue and moves it to Shipped when the PR merges. For earlier stages, status is updated directly on the issue page: when work is committed to a milestone it moves to Planned, and to In Progress when someone picks it up. If a PR is related to an issue but doesn't fully resolve it, referencing the issue number without `Closes` connects the history and keeps the issue open.

Milestones represent planned scope for a release. An issue without a milestone is either unplanned or declined. If something is explicitly won't-fix or a duplicate, it should get a `wontfix` or `duplicate` label, the milestone is removed, and is closed. That keeps milestone views in the Project clean without needing a separate status for declined work.

### Community

We can be found on Discord and GitHub Discussions. As an open source project, we survive and thrive on new voices and new contributors. If you need help using VillageSQL, have a bug report or a feature request, or just need a little extra help with your first contribution, we're more than happy to talk to you!

- 💬 [Discord](https://discord.gg/KSr6whd3Fr)
- 🏗️ [Discussions](https://github.com/villagesql/villagesql-server/discussions)
- 🌐 [Website](https://villagesql.com)
