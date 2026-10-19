#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Check out a tag of the source tree, build it into a fresh (or given) build
# directory, then run the full MTR suite against that build.
#
# Logs are written to <build-dir>/logs/. Run with --help for options.

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "$TOOLS_DIR/discover_build_env.sh"

usage() {
    cat <<EOF
Usage: $(basename "$0") --tag TAG [options]

Check out TAG, build it, and run the full MTR suite.

Required:
  --tag TAG           Git tag (or other ref) to check out and build.

Options:
  --src-dir DIR       Source tree to build (default: current directory).
  --base-dir DIR      Parent directory for a new timestamped build directory
                      (default: \$BASE_DIR if set, else \$HOME/build).
  --build-dir DIR     Exact build directory; overrides --base-dir.
  --build-type TYPE   CMake build type (default: debug).
  --jobs N            Parallel build jobs (default: number of online CPUs).
  --skip-build        Do not configure/build; just run MTR. Requires
                      --build-dir, pointing at an existing build.
  --clean             Wipe the build directory before configuring.
  --help              Show this help.
EOF
}

# Print an absolute form of path $1 without requiring it to exist.
abs_path() {
    local path="$1"
    if [[ "$path" == /* ]]; then
        echo "$path"
    else
        echo "$PWD/$path"
    fi
}

# Print the -DWITH_SSL value for os, or fail if OpenSSL is missing.
# macOS pins Homebrew's openssl@3: the unversioned alias now points at
# openssl@4, which MySQL 8.4 does not build against.
# Args: os
find_with_ssl() {
    local os="$1"

    case "$os" in
        macos)
            local dir
            dir="$(brew --prefix openssl@3)"
            [[ -d "$dir" ]] || { echo "Missing dependency: $dir (brew install openssl@3)" >&2; return 1; }
            echo "$dir"
            ;;
        linux)
            echo "system"
            ;;
        *)
            echo "Unsupported build OS: $os" >&2
            return 1
            ;;
    esac
}

# Print the bison executable for os, or fail if it is missing.
# macOS ships bison 2.3, which is too old, so use Homebrew's.
# Args: os
find_bison_bin() {
    local os="$1"
    local bin=""

    case "$os" in
        macos) bin="$(brew --prefix bison)/bin/bison" ;;
        linux) bin="$(command -v bison || true)" ;;
        *)
            echo "Unsupported build OS: $os" >&2
            return 1
            ;;
    esac
    [[ -n "$bin" && -x "$bin" ]] || { echo "Missing dependency: bison${bin:+ ($bin)}" >&2; return 1; }
    echo "$bin"
}

# Check out tag in src_dir. Refuses if tracked files have uncommitted
# changes, so the build is exactly the tag.
# Args: src_dir tag
checkout_source() {
    local src_dir="$1"
    local tag="$2"

    if [[ -n "$(git -C "$src_dir" status --porcelain --untracked-files=no)" ]]; then
        echo "Uncommitted changes in $src_dir; commit or stash before checking out $tag" >&2
        return 1
    fi
    echo ">> Checking out $tag in $src_dir"
    git -C "$src_dir" checkout "$tag"
}

# Create bld_dir/logs/, wiping bld_dir first if clean is 1.
# Args: bld_dir clean
prepare_build_dir() {
    local bld_dir="$1"
    local clean="$2"

    if [[ $clean -eq 1 && -d "$bld_dir" ]]; then
        echo ">> Cleaning $bld_dir"
        rm -rf "$bld_dir"
    fi
    mkdir -p "$bld_dir/logs"
}

# Configure and compile src_dir into bld_dir. Logs go to bld_dir/logs/.
# Args: src_dir bld_dir build_type jobs with_ssl bison_bin timestamp
build_codebase() {
    local src_dir="$1"
    local bld_dir="$2"
    local build_type="$3"
    local jobs="$4"
    local with_ssl="$5"
    local bison_bin="$6"
    local timestamp="$7"

    echo ">> Source: $src_dir ($(git -C "$src_dir" log -1 --format='%h %s'))"
    local cmake_log="$bld_dir/logs/cmake-$timestamp.log"
    echo ">> Configuring in $bld_dir (log: $cmake_log)"
    # CMAKE_POLICY_VERSION_MINIMUM: CMake 4.x rejects bundled third-party
    # projects (e.g. libcbor) that declare cmake_minimum_required < 3.5.
    cmake -S "$src_dir" -B "$bld_dir" \
        -DCMAKE_BUILD_TYPE="$build_type" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DWITH_SSL="$with_ssl" \
        -DBISON_EXECUTABLE="$bison_bin" \
        -DWITH_UNIT_TESTS=OFF \
        2>&1 | tee "$cmake_log"

    local build_log="$bld_dir/logs/build-$timestamp.log"
    echo ">> Building with $jobs jobs (log: $build_log)"
    cmake --build "$bld_dir" --parallel "$jobs" \
        2>&1 | tee "$build_log"
}

# Run the full MTR suite in bld_dir. Returns MTR's exit code.
# Args: bld_dir timestamp
run_mtr() {
    local bld_dir="$1"
    local timestamp="$2"

    # MTR with --parallel=auto opens many files/sockets.
    ulimit -n 65536 2>/dev/null || ulimit -n "$(ulimit -Hn)" || true

    local mtr_log="$bld_dir/logs/mtr-$timestamp.log"
    local result_file="$bld_dir/result-$timestamp.xml"
    echo ">> Running MTR in $bld_dir (log: $mtr_log)"
    cd "$bld_dir"
    set +e
    ./mysql-test/mysql-test-run.pl \
        --mem \
        --report-unstable-tests \
        --parallel=auto \
        --nounit-tests \
        --force \
        --max-test-fail=0 \
        --suite=all \
        --xml-report="$result_file" \
        2>&1 | tee "$mtr_log"
    local rc=${PIPESTATUS[0]}
    set -e

    echo ">> MTR exit code: $rc (log: $mtr_log)"
    echo "result-file=$result_file"
    return "$rc"
}

main() {
    local tag=""
    local src_dir="."
    local base_dir="${BASE_DIR:-$HOME/build}"
    local bld_dir=""
    local build_type="debug"
    local jobs="$(getconf _NPROCESSORS_ONLN)"
    local skip_build=0
    local clean=0

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --tag)        tag="${2:?--tag requires a value}"; shift 2 ;;
            --src-dir)    src_dir="${2:?--src-dir requires a value}"; shift 2 ;;
            --base-dir)   base_dir="${2:?--base-dir requires a value}"; shift 2 ;;
            --build-dir)  bld_dir="${2:?--build-dir requires a value}"; shift 2 ;;
            --build-type) build_type="${2:?--build-type requires a value}"; shift 2 ;;
            --jobs)       jobs="${2:?--jobs requires a value}"; shift 2 ;;
            --skip-build) skip_build=1; shift ;;
            --clean)      clean=1; shift ;;
            --help|-h)    usage; return 0 ;;
            *) echo "Unknown option: $1" >&2; usage >&2; return 2 ;;
        esac
    done

    if [[ -z "$tag" ]]; then
        echo "--tag is required" >&2
        usage >&2
        return 2
    fi
    if [[ $skip_build -eq 1 && -z "$bld_dir" ]]; then
        echo "--skip-build requires --build-dir" >&2
        return 2
    fi

    [[ -d "$src_dir" ]] || { echo "No such source directory: $src_dir" >&2; return 2; }
    src_dir="$(cd "$src_dir" && pwd)"

    local timestamp
    timestamp="$(date +%m%d%H%M)"
    if [[ -z "$bld_dir" ]]; then
        bld_dir="$(abs_path "$base_dir")/${tag}_${timestamp}_${build_type}"
    else
        bld_dir="$(abs_path "$bld_dir")"
    fi

    # Find dependencies before touching the worktree, so a missing one fails fast.
    local with_ssl="" bison_bin=""
    if [[ $skip_build -eq 0 ]]; then
        local os
        os="$(discover_build_env)"
        with_ssl="$(find_with_ssl "$os")"
        bison_bin="$(find_bison_bin "$os")"
    fi

    checkout_source "$src_dir" "$tag"
    prepare_build_dir "$bld_dir" "$clean"

    if [[ $skip_build -eq 0 ]]; then
        build_codebase "$src_dir" "$bld_dir" "$build_type" "$jobs" \
            "$with_ssl" "$bison_bin" "$timestamp"
    fi

    run_mtr "$bld_dir" "$timestamp"
}

main "$@"
