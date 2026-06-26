#!/usr/bin/env python3
# Copyright (c) 2026 VillageSQL Contributors
"""
Regression test framework for villint.sh

This script creates a temporary git worktree and runs test cases to verify
that villint.sh correctly fixes various style guide violations.

Each test case named <foo> comprises:
  - <name>_edit.sh: Creates a style violation or makes a valid change
  - <name>_verify.sh: Checks if the code meets the style guide (exit 0 = pass, 1 = fail)
  - <name>_properties.env: Optional properties file with test configuration
  - <name>_commit.txt: Optional commit message to create after edit script runs

Properties file options:
  UNFIXABLE=true       Expects villint.sh to fail (cannot auto-fix). Verify should fail both before and after.
  NO_VIOLATION=true    Makes a valid change (no violation). Verify should pass before villint runs.
  PRESERVE_MTIME=true  Verifies that files without content changes have preserved mtimes.

Standard test procedure:
* Reset to clean state
* Run edit script (should create violation)
* If commit.txt exists: Create commit with that message
* Run verify script (should fail - exit 1)
* If PRESERVE_MTIME=true: Record mtimes of files villint will process
* Run villint.sh (should succeed and fix violation)
* If PRESERVE_MTIME=true: Verify unchanged files have unchanged mtimes
* Run verify script (should pass - exit 0)
* Run villint.sh again
* Verify no changes were made (should be idempotent)
"""

import argparse
import os
import subprocess
import sys
import tempfile
import shutil
from pathlib import Path
from typing import List, Tuple


class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def run_command(cmd: List[str], cwd: str, check: bool = True) -> Tuple[int, str, str]:
    """Run a command and return (returncode, stdout, stderr)"""
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, cmd, result.stdout, result.stderr
        )
    return result.returncode, result.stdout, result.stderr


def print_status(message: str, color: str = Colors.BLUE):
    """Print a status message with color"""
    print(f"{color}{message}{Colors.RESET}")


def print_success(message: str):
    """Print a success message"""
    print(f"{Colors.GREEN}✓ {message}{Colors.RESET}")


def print_error(message: str):
    """Print an error message"""
    print(f"{Colors.RED}✗ {message}{Colors.RESET}")


def print_warning(message: str):
    """Print a warning message"""
    print(f"{Colors.YELLOW}⚠ {message}{Colors.RESET}")


class TestCase:
    """Represents a single villint test case"""

    def __init__(self, name: str, edit_script: Path, verify_script: Path, properties_file: Path = None, commit_file: Path = None):
        self.name = name
        self.edit_script = edit_script
        self.verify_script = verify_script
        self.properties_file = properties_file
        self.commit_file = commit_file
        self.unfixable = False
        self.no_violation = False
        self.preserve_mtime = False

        # Load properties if file exists
        if properties_file and properties_file.exists():
            self._load_properties()

    def _load_properties(self):
        """Load properties from the properties file"""
        with open(self.properties_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        if key == 'UNFIXABLE':
                            self.unfixable = value.lower() in ('true', '1', 'yes')
                        elif key == 'NO_VIOLATION':
                            self.no_violation = value.lower() in ('true', '1', 'yes')
                        elif key == 'PRESERVE_MTIME':
                            self.preserve_mtime = value.lower() in ('true', '1', 'yes')

    def __str__(self):
        return self.name


class VillintTestRunner:
    """Runs villint regression tests"""

    def __init__(self, source_dir: Path, base_branch: str = "origin/main", debug: bool = False):
        self.source_dir = source_dir
        self.base_branch = base_branch
        self.villint_script = source_dir / "scripts" / "villint.sh"
        self.test_cases_dir = source_dir / "villagesql" / "villint" / "regtest_villint_test_cases"
        self.temp_dir = None
        self.test_branch = None
        self.debug = debug

    def discover_test_cases(self) -> List[TestCase]:
        """Discover all test cases in the test_cases directory"""
        test_cases = {}

        if not self.test_cases_dir.exists():
            print_error(f"Test cases directory not found: {self.test_cases_dir}")
            return []

        for script_file in self.test_cases_dir.glob("*_edit.sh"):
            # Extract test name by removing _edit.sh suffix
            test_name = script_file.stem[:-5]  # Remove _edit
            verify_file = self.test_cases_dir / f"{test_name}_verify.sh"
            properties_file = self.test_cases_dir / f"{test_name}_properties.env"
            commit_file = self.test_cases_dir / f"{test_name}_commit.txt"

            if verify_file.exists():
                test_cases[test_name] = TestCase(
                    test_name,
                    script_file,
                    verify_file,
                    properties_file if properties_file.exists() else None,
                    commit_file if commit_file.exists() else None
                )
            else:
                print_warning(f"Found {script_file.name} but no matching verify script")

        return list(test_cases.values())

    def setup_test_repo(self):
        """Create a temporary git clone for testing"""
        print_status(f"Setting up test repository...")

        # Resolve base_branch to a commit SHA in the source repo BEFORE cloning.
        # This is necessary because git clone creates origin/* refs from the
        # source repo's BRANCHES, not its remote-tracking branches. So the clone's
        # origin/main would be the source's local "main" branch, not "origin/main".
        returncode, stdout, stderr = run_command(
            ["git", "rev-parse", self.base_branch],
            cwd=str(self.source_dir),
        )
        self.base_commit = stdout.strip()

        # Generate random suffix for directory name
        import random
        import string
        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        dir_name = f"villint_regtest_{random_suffix}"
        self.test_branch = None  # Not using branches with clone

        # Create temp directory path
        self.temp_dir = Path("/tmp") / dir_name
        print_status(f"  Creating clone: {self.temp_dir}")

        # Use git clone --shared instead of worktree to avoid VSCode tracking.
        # Worktrees are registered in .git/worktrees/ which VSCode monitors, causing
        # "too many active changes" popups. Shared clones reference the same objects
        # but are independent repos, so VSCode doesn't see them.
        run_command(
            ["git", "clone", "--shared", "--no-checkout", str(self.source_dir), str(self.temp_dir)],
            cwd="/tmp",
        )

        # Checkout the resolved commit (not the ref name, see comment above)
        run_command(
            ["git", "checkout", self.base_commit],
            cwd=str(self.temp_dir),
        )

        # Fix the origin/main ref in the clone to point to the correct commit.
        # Without this, villint.sh's merge-base calculation would use the wrong
        # origin/main and see too many files as "changed".
        run_command(
            ["git", "update-ref", "refs/remotes/origin/main", self.base_commit],
            cwd=str(self.temp_dir),
        )

        print_success("Test repository setup complete")

    def cleanup_test_repo(self):
        """Remove test clone directory"""
        if self.temp_dir and self.temp_dir.exists():
            print_status(f"Removing test clone: {self.temp_dir}")
            try:
                import shutil
                shutil.rmtree(self.temp_dir)
            except Exception as e:
                print_warning(f"Could not remove test clone: {e}")

    def reset_to_clean_state(self):
        """Reset the test repository to a clean state"""
        # Reset to the base commit (resolved from origin/main), not HEAD
        # This ensures commits from previous tests don't pollute subsequent tests
        run_command(["git", "reset", "--hard", self.base_commit], cwd=str(self.temp_dir))
        run_command(["git", "clean", "-fd"], cwd=str(self.temp_dir))

    def run_test_case(self, test_case: TestCase) -> bool:
        """Run a single test case. Returns True if passed, False if failed."""
        print_status(f"\n{'='*60}")
        print_status(f"Running test: {Colors.BOLD}{test_case.name}{Colors.RESET}")
        print_status(f"{'='*60}")

        if self.debug:
            print_status(f"Debug mode: Worktree at {self.temp_dir}")

        if test_case.unfixable:
            print_warning(f"Test marked as UNFIXABLE (expects villint.sh to fail)")
        if test_case.no_violation:
            print_warning(f"Test marked as NO_VIOLATION (no violation expected)")

        try:
            # Reset to clean state
            print_status("Resetting to clean state...")
            self.reset_to_clean_state()
            print_success("  Reset complete")

            # Run edit script
            if test_case.no_violation:
                print_status("Running edit script (making valid change)...")
            else:
                print_status("Running edit script (creating violation)...")
            returncode, stdout, stderr = run_command(
                ["bash", str(test_case.edit_script)],
                cwd=str(self.temp_dir),
            )
            if stdout:
                print(f"  {stdout}")
            print_success("  Edit script complete")

            # Create commit if commit file exists
            if test_case.commit_file:
                print_status("Creating commit with message from commit file...")

                # Stage all changes
                run_command(["git", "add", "-A"], cwd=str(self.temp_dir))

                # Check if there are changes to commit
                returncode, stdout, stderr = run_command(
                    ["git", "diff", "--cached", "--quiet"],
                    cwd=str(self.temp_dir),
                    check=False,
                )

                if returncode != 0:  # There are staged changes
                    # Read commit message from file
                    with open(test_case.commit_file, 'r') as f:
                        commit_message = f.read().strip()

                    # Create the commit
                    run_command(
                        ["git", "commit", "-m", commit_message],
                        cwd=str(self.temp_dir),
                    )
                    print_success(f"  Commit created")
                else:
                    print_warning("  No changes to commit (edit script made no modifications)")

            # Run verify script - should FAIL (violation exists) unless NO_VIOLATION
            if test_case.no_violation:
                print_status("Verifying no violation exists...")
                returncode, stdout, stderr = run_command(
                    ["bash", str(test_case.verify_script)],
                    cwd=str(self.temp_dir),
                    check=False,
                )
                if returncode != 0:
                    print_error("  Verify script failed but should have passed!")
                    print_error("  Edit script should not have created a violation")
                    return False
                print_success("  No violation detected (as expected)")
            else:
                print_status("Verifying violation exists...")
                returncode, stdout, stderr = run_command(
                    ["bash", str(test_case.verify_script)],
                    cwd=str(self.temp_dir),
                    check=False,
                )
                if returncode == 0:
                    print_error("  Verify script passed but should have failed!")
                    print_error("  Edit script did not create a violation")
                    return False
                print_success("  Violation detected (verify script failed as expected)")

            # Record mtimes if PRESERVE_MTIME is set
            mtimes_before = {}
            if test_case.preserve_mtime:
                print_status("Recording mtimes for files villint will process...")

                # Get the list of files villint will process (mimics villint.sh logic)
                # Tracked files that have been modified/added
                returncode, tracked_files, _ = run_command(
                    ["git", "diff", "--name-only", "--diff-filter=d", "HEAD"],
                    cwd=str(self.temp_dir),
                    check=False,
                )
                # New untracked files
                returncode, untracked_files, _ = run_command(
                    ["git", "ls-files", "--others", "--exclude-standard"],
                    cwd=str(self.temp_dir),
                    check=False,
                )

                files_to_check = []
                if tracked_files.strip():
                    files_to_check.extend(tracked_files.strip().split('\n'))
                if untracked_files.strip():
                    files_to_check.extend(untracked_files.strip().split('\n'))

                for file_path in files_to_check:
                    full_path = self.temp_dir / file_path
                    if full_path.exists() and full_path.is_file():
                        mtimes_before[file_path] = full_path.stat().st_mtime

                print_success(f"  Recorded mtimes for {len(mtimes_before)} file(s)")

            # Run villint.sh
            print_status("Running villint.sh...")
            returncode, stdout, stderr = run_command(
                ["bash", str(self.villint_script)],
                cwd=str(self.temp_dir),
                check=False,
            )
            if stdout:
                print(f"  {stdout}")
            if stderr:
                print(f"  {stderr}")

            # Check mtime preservation if requested
            if test_case.preserve_mtime and mtimes_before:
                print_status("Verifying mtimes were preserved...")

                files_with_mtime_changes = []

                for file_path, old_mtime in mtimes_before.items():
                    full_path = self.temp_dir / file_path
                    if full_path.exists():
                        new_mtime = full_path.stat().st_mtime
                        if old_mtime != new_mtime:
                            # Check if file had content changes
                            returncode, _, _ = run_command(
                                ["git", "diff", "--quiet", file_path],
                                cwd=str(self.temp_dir),
                                check=False,
                            )
                            if returncode == 0:
                                # No content changes but mtime changed!
                                files_with_mtime_changes.append(file_path)

                if files_with_mtime_changes:
                    print_error(f"  Files had mtime changes without content changes:")
                    for f in files_with_mtime_changes:
                        print_error(f"    - {f}")
                    return False

                print_success("  All file mtimes preserved")

            if test_case.unfixable:
                # For unfixable tests, we expect villint.sh to fail
                if returncode != 0:
                    print_success("  villint.sh failed as expected (unfixable violation)")

                    # Verify script should still fail (violation not fixed)
                    print_status("Verifying violation was not fixed...")
                    returncode, stdout, stderr = run_command(
                        ["bash", str(test_case.verify_script)],
                        cwd=str(self.temp_dir),
                        check=False,
                    )
                    if returncode != 0:
                        print_success("  Violation still exists (as expected)")
                        print_success(f"\n{Colors.BOLD}Test {test_case.name} PASSED{Colors.RESET}")
                        return True
                    else:
                        print_error("  Verify script passed but should have failed!")
                        print_error("  Violation should not have been fixed")
                        return False
                else:
                    print_error("  villint.sh succeeded but should have failed!")
                    print_error("  This is an unfixable test case")
                    return False
            else:
                # For fixable tests, we expect villint.sh to succeed
                if returncode != 0:
                    print_error("  villint.sh failed unexpectedly")
                    return False

                print_success("  villint.sh complete")

                # Run verify script - should PASS (violation fixed)
                print_status("Verifying violation was fixed...")
                returncode, stdout, stderr = run_command(
                    ["bash", str(test_case.verify_script)],
                    cwd=str(self.temp_dir),
                )
                print_success("  Violation fixed (verify script passed)")

                # Verify idempotency - commit fixes and run again on clean state
                print_status("Verifying villint.sh is idempotent...")

                # Commit the fixes from the first run
                run_command(["git", "add", "-A"], cwd=str(self.temp_dir))
                run_command(["git", "commit", "-m", "Apply villint fixes"], cwd=str(self.temp_dir), check=False)

                # Run villint.sh again on the committed, fixed files
                returncode, stdout, stderr = run_command(
                    ["bash", str(self.villint_script)],
                    cwd=str(self.temp_dir),
                )

                # Check if any changes were made
                returncode, stdout, stderr = run_command(
                    ["git", "status", "--porcelain"],
                    cwd=str(self.temp_dir),
                )

                if stdout.strip():
                    print_error("  villint.sh made changes on second run!")
                    print_error("  villint.sh is not idempotent")
                    print(f"  Changes:\n{stdout}")
                    return False

                print_success("  villint.sh is idempotent (no changes on second run)")

                print_success(f"\n{Colors.BOLD}Test {test_case.name} PASSED{Colors.RESET}")
                return True

        except subprocess.CalledProcessError as e:
            print_error(f"\nCommand failed: {' '.join(e.cmd)}")
            print_error(f"Return code: {e.returncode}")
            if e.stdout:
                print_error(f"stdout: {e.stdout}")
            if e.stderr:
                print_error(f"stderr: {e.stderr}")
            print_error(f"\n{Colors.BOLD}Test {test_case.name} FAILED{Colors.RESET}")
            if self.debug:
                print_warning(f"\nDebug mode: Worktree preserved at {self.temp_dir}")
                print_warning(f"Branch: {self.test_branch}")
                print_warning("Run cleanup manually when done:")
                print_warning(f"  git worktree remove {self.temp_dir} --force")
                print_warning(f"  git branch -D {self.test_branch}")
            return False
        except Exception as e:
            print_error(f"\nUnexpected error: {e}")
            print_error(f"\n{Colors.BOLD}Test {test_case.name} FAILED{Colors.RESET}")
            if self.debug:
                print_warning(f"\nDebug mode: Worktree preserved at {self.temp_dir}")
                print_warning(f"Branch: {self.test_branch}")
            return False

    def run_all_tests(self, test_filter: str = None) -> int:
        """Run all test cases. Returns number of failed tests."""
        # Check for stale villint test directories
        print_status("Checking for stale test directories...")
        import glob
        stale_dirs = glob.glob("/tmp/villint_regtest_*") + glob.glob("/private/tmp/villint_regtest_*")

        if stale_dirs:
            print_error("Found stale villint test directories:")
            for d in stale_dirs:
                print_error(f"  - {d}")
            print_error("\nThese are likely from a previous interrupted test run.")
            print_error("Please clean them up manually with:")
            print_error(f"  rm -rf /tmp/villint_regtest_*")
            return 1

        print_success("No stale test directories found")

        test_cases = self.discover_test_cases()

        if not test_cases:
            print_error("No test cases found!")
            return 1

        # Filter test cases if specified
        if test_filter:
            test_cases = [tc for tc in test_cases if tc.name == test_filter]
            if not test_cases:
                print_error(f"No test case found matching: {test_filter}")
                return 1
            print_status(f"\nRunning filtered test: {test_filter}")
        else:
            print_status(f"\nDiscovered {len(test_cases)} test case(s):")
            for tc in test_cases:
                print(f"  - {tc.name}")

        try:
            self.setup_test_repo()

            passed = 0
            failed = 0

            for test_case in test_cases:
                if self.run_test_case(test_case):
                    passed += 1
                else:
                    failed += 1
                    if self.debug:
                        # Stop on first failure in debug mode
                        break

            # Print summary
            print_status(f"\n{'='*60}")
            print_status(f"{Colors.BOLD}Test Summary{Colors.RESET}")
            print_status(f"{'='*60}")
            print_success(f"Passed: {passed}/{len(test_cases)}")
            if failed > 0:
                print_error(f"Failed: {failed}/{len(test_cases)}")
            else:
                print_success("All tests passed!")

            return failed

        finally:
            if not (self.debug and failed > 0):
                self.cleanup_test_repo()
            else:
                print_warning("\nDebug mode: Skipping cleanup to preserve worktree")


def main():
    parser = argparse.ArgumentParser(
        description="Run villint.sh regression tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path(__file__).parent.parent.parent, # ./villagesql/villint
        help="Path to source repository (default: grandparent of script directory)",
    )
    parser.add_argument(
        "--base-branch",
        type=str,
        default="origin/main",
        help="Base branch to branch from (default: origin/main)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Debug mode: stop on first failure and preserve worktree for investigation",
    )
    parser.add_argument(
        "--test",
        type=str,
        help="Run only the specified test case (by name)",
    )

    args = parser.parse_args()

    if not args.source_dir.exists():
        print_error(f"Source directory does not exist: {args.source_dir}")
        return 1

    runner = VillintTestRunner(args.source_dir, args.base_branch, args.debug)
    failed_count = runner.run_all_tests(test_filter=args.test)

    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
