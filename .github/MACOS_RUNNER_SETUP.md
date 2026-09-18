# Setting up a Mac mini as a self-hosted GitHub Actions runner

This guide walks through the steps to setup a self-hosted
GitHub Actions runner for VillageSQL Server on a Mac. GitHub automatically gives the
runner the labels `self-hosted`, `macOS`, and `ARM64`, which is what our
workflows target, so there is nothing extra to configure on that front.

## Prerequisites

- An Apple Silicon Mac (M1 or newer) running a recent macOS.
- A regular (non-root) user account on that Mac. Everything below runs as
  this user.
- Admin access to the `villagesql/villagesql-server` repository. You need it
  to reach the runner settings page and generate a registration token.
- The [GitHub CLI](https://cli.github.com/) (`gh`) on whatever machine you
  use to kick off test workflows.

## 1. Prepare the machine

Install the Xcode command line tools and Homebrew. Homebrew should end up
owned by your user rather than root, otherwise the runner won't be able to
install packages later.

```bash
xcode-select --install

/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"
```

Check that both worked:

```bash
brew --version          # should run without sudo
ls -ld /opt/homebrew    # should be owned by your user, not root
```

## 2. Register the runner with GitHub

In the repository, go to **Settings → Actions → Runners → New self-hosted
runner** and pick **macOS** and **arm64**. That page gives you three things:
the download URL for the current runner release, a registration token, and
the `config.sh` command to run. The token is only valid for about an hour, so
generate it when you're ready to use it.

The commands below are the same ones from that page. Substitute the version
and token you see there.

```bash
mkdir -p ~/actions-runner && cd ~/actions-runner
curl -o actions-runner-osx-arm64.tar.gz -L \
  https://github.com/actions/runner/releases/download/v2.335.1/actions-runner-osx-arm64-2.335.1.tar.gz
tar xzf ./actions-runner-osx-arm64.tar.gz

./config.sh \
  --url https://github.com/villagesql/villagesql-server \
  --token <TOKEN> \
  --name mac-mini-1
```

Accept the defaults when `config.sh` prompts you. Pick a `--name` that
identifies the physical machine, since that's what shows up in the Runners
list.

Note that a runner belongs to exactly one repository or organization. If you
want to run workflows from a different repo, you'll need to register it there.

## 3. Run it as a service

The runner ships with a helper script, `svc.sh`, that installs it as a
per-user LaunchAgent. This means it starts automatically when you log in and
restarts itself if it crashes or loses its connection to GitHub. None of this
needs `sudo`.

Before starting the service, add Homebrew to the runner's `PATH`. The runner
reads its `PATH` from a `.path` file at install time, and Homebrew isn't in it
by default.

```bash
cd ~/actions-runner
./svc.sh install

printf '/opt/homebrew/bin:/opt/homebrew/sbin:%s' "$(cat .path)" > .path.tmp && mv .path.tmp .path

./svc.sh start
./svc.sh status
```

`status` should report `Started`, and the runner should appear as **Idle** on
the Runners page in GitHub.

To manage the service later:

```bash
./svc.sh stop
./svc.sh start
./svc.sh status
./svc.sh uninstall
```

## 4. Keep it running unattended

A LaunchAgent only runs while a user is logged in, so a few macOS settings
need to change for the runner to survive reboots and idle periods without
someone at the keyboard.

First, stop the machine from sleeping and have it come back on its own after
a power loss:

```bash
sudo pmset -a sleep 0 disksleep 0
sudo pmset -a autorestart 1
```

Then turn on automatic login. Go to **System Settings → Users & Groups** and
set **Automatically log in as** to the runner's user account. Without this,
the runner won't start after a reboot until someone logs in.

## 5. Try it out

Dispatch a workflow that targets the macOS runner and watch for it to pick up
the job. Use `--ref` to point at whichever branch you want to test.

```bash
gh workflow run extension-compat.yml \
  --repo villagesql/villagesql-server \
  --ref main \
  -f platform=macos-arm64

gh run list --workflow extension-compat.yml --repo villagesql/villagesql-server
```

## Running more than one job at a time

A single runner handles one job at a time. Any additional jobs queue up and
run one after another. If you need more throughput, you have two options:

- **Run several runners on one Mac.** Extract the runner into a separate
  directory, give it a different `--name`, and install it as a second
  service. Each runner keeps its own `_work` directory but they share the
  same Homebrew install. Keep in mind they compete for CPU and memory, so
  this works well for light jobs and less well for several full builds at
  once.
- **Add more Macs.** Register each one with the same steps above. Since they
  all get the same labels, GitHub spreads jobs across them automatically.

## Troubleshooting

**The runner shows as offline, or the log says "Runner not found".**
The runner's session with GitHub expired, usually because the machine slept
or lost network. The service should restart it on its own. If the runner was
removed from the GitHub side, re-run `config.sh` with a fresh token.

**I want to test against my fork.**
Register the runner against your fork's URL instead, and dispatch workflows
with `--repo <your-username>/villagesql-server`. Remember that a runner can
only be attached to one repository at a time, so you'll need to re-register
it to move it back.
