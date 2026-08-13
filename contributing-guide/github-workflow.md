# GitHub Workflow

This guide covers the preferred GitHub workflow for VillageSQL, including best practices for keeping your environment in sync with the upstream project and maintaining good commit hygiene for easier contribution.

## 1. Fork the Project

1. While logged into your GitHub account, go to https://github.com/villagesql/villagesql-server

2. In the upper right-hand corner, click the "Fork" button. You do not need to change the name of the repository. You now have a copy of the `villagesql-server` repository in your own GitHub account, at `https://github.com<your-username>/villagesql-server`.

## 2. Clone your Fork

Open your terminal and run the following commands to pull down a copy of your VillageSQL Server fork and confirm it's set up correctly.

```sh
git clone https://github.com/your-username/villagesql-server.git
# or: git clone git@github.com:your-username/villagesql.git

cd villagesql-server
git remote add upstream https://github.com/villagesql/villagesql-server.git
# or: git remote add upstream git@github.com:villagesql/villagesql-server.git

# Disallow pushing to the upstream main branch
git remote set-url --push upstream no_push

# Confirm that your fork is listed as the origin and villagesql/villagesql-server.git is listed as the upstream
git remote -v
```

## 3. Create a Feature Branch

First, ensure that your local main branch is up to date with upstream.

```sh
cd villagesql-server
git fetch upstream
git checkout main
git rebase upstream/main
```

Create your new branch, ideally with a name relevant to the work you are doing. The following command creates a new branch called `myfeature`, and the `-b` flag switches you to that new branch.

```sh
git checkout -b myfeature
```

## 4. Staying in Sync

To make sure your feature branch remains in sync and avoid the need to manage merge conflicts, periodically fetch changes from upstream by running the following commands from your feature branch:

```sh
git fetch upstream
git rebase upstream/main
```

Using `fetch` and then `rebase` as above is preferable to using `git pull`. While both work to keep your brach in sync, running `git pull` creates a merge commit instead of preserving individual commits. This makes the commit history harder to read and less useful.

## 5. Run the Linter

VillageSQL includes a linter in the scripts directory to assist with some minor style and convention compliance. Run it right before committing and pushing your changes.

```sh
./scripts/villint.sh
```

If you do not have its dependencies installed, you will be prompted to do so in your terminal.


## 6. Commit Your Changes

Commit messages should be brief but useful. We require 41 characters or less, with no period or full-stop at the end of the commit message. The `-m` flag allows you to include a commit message.

```sh
git commit -m "<your commit message here>"
```

## 7. Push to GitHub

When your changes are ready for review, push your working branch to
your fork on GitHub.

```sh
git push -f <your_remote_name> myfeature
```

## 8. Create a Pull Request

1. Navigate to your fork at `https://github.com/<user>/villagesql-server`
2. Click the **Compare & Pull Request** button next to your feature branch.
3. Ensure that the the base repository is `villagesql/villagesql-server` and the base is `main`. The head respository should be `<user>/villagesql-server` and the compare dropdown should be your feature branch.
4. Add a thorough, clear description of the content of your commits to the PR description. Include changes made, such as features added, bugs fixed, or performance improvements. If your PR resolves a known issue, include `Closes #<issue number>`.
5. If you have used generative AI in your workflow, disclose it in the PR description along with the name of the tool. For example, "This PR used generative AI.""


## Code Review Process

Next, your pull request will have one or more reviewers assigned to it. They may suggest changes or improvements; this is not a judgment on the value of your contribution, only an effort to help make the project the best it can be.

Reviews may take some time. Your reviewers are working as quickly as they can, but may have many PRs to review. If your PR is small this process may be very quick, but if it is particularly large, please be patient and understand that reviewing more content takes more time.
