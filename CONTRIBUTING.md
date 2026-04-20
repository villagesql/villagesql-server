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

1. File a Github [issue](./issues) for any improvement or feature request
2. All requests will be reviewed weekly by the Village council (i.e. committers)
3. Once a Github [issue](./issues) and solution has been agreed to, submit a pull request including signing the CLA

We look forward to hearing from you.
