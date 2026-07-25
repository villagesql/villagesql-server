#!/bin/bash
# create_malformed_veb.sh - Create malformed VEB archives for testing
#
# This script creates VEB files with arbitrary content for testing error handling
# and security. For building real extensions with C++ code, use create_extension_sdk.inc
# which builds extensions using the SDK workflow.
#
# Usage: create_malformed_veb.sh <extension_name> [options...]
#
# Options:
#   --file <filename> <content>    Add a file with specified content
#   --filename <source_file>       Copy an existing file into the VEB
#   --symlink <name> <target>      Create a symlink (for security testing)
#   --hardlink <name> <target>     Create a hardlink (for security testing)
#
# Examples:
#   # Create VEB with bad manifest for error testing
#   create_malformed_veb.sh bad_manifest --file manifest.json '{invalid json'
#
#   # Create VEB with symlink for security testing
#   create_malformed_veb.sh symlink_attack \
#     --file manifest.json '{"version": "1.0.0"}' \
#     --symlink evil_link ../../../etc/passwd

set -euo pipefail

# Strip LD_PRELOAD for this script's tooling. On sanitizer builds MTR's
# safe_process LD_PRELOADs the shared ASan runtime into every child process,
# and it propagates through mysqltest into this script. The uninstrumented
# system tar/gzip then run under LeakSanitizer, which flags their own internal
# allocations at exit and fails with LSAN_OPTIONS=exitcode=42. Unsetting it
# only removes the preloaded runtime; it adds no instrumentation, so the VEB
# produced is identical while avoiding the spurious leak reports.
unset LD_PRELOAD

if [ $# -lt 2 ]; then
    echo "Usage: $0 <extension_name> [options...]" >&2
    echo "" >&2
    echo "Creates a VEB archive with the specified files for testing." >&2
    echo "For building extensions with C++ code, use create_extension_sdk.inc instead." >&2
    echo "" >&2
    echo "Options:" >&2
    echo "  --file <filename> <content>       Add a file with the given content" >&2
    echo "  --filename <source_file>          Copy an existing file into the VEB" >&2
    echo "  --symlink <link_name> <target>    Create a symlink (for security testing)" >&2
    echo "  --hardlink <link_name> <target>   Create a hardlink (for security testing)" >&2
    echo "" >&2
    echo "Examples:" >&2
    echo "  $0 bad_ext --file manifest.json '{invalid}'" >&2
    echo "  $0 attack --file manifest.json '{\"version\": \"1.0.0\"}' --symlink evil ../../../etc/passwd" >&2
    exit 1
fi

extension_name="$1"
shift

# Create temporary directory for building the archive
temp_dir=$(mktemp -d)
trap "rm -rf '$temp_dir'" EXIT

# Parse arguments
while [ $# -gt 0 ]; do
    case "$1" in
        --file)
            if [ $# -lt 3 ]; then
                echo "Error: --file requires both filename and content arguments" >&2
                exit 1
            fi

            filename="$2"
            content="$3"
            shift 3

            # Validate filename (block .. and leading /, but allow subdirectories)
            if [[ "$filename" == *".."* || "$filename" == "." || "$filename" == "/" || "$filename" == /* ]]; then
                echo "Error: Filename cannot contain '..' or be '.' or start with '/'" >&2
                exit 1
            fi

            # Create parent directory if filename contains subdirectories
            file_dir=$(dirname "$temp_dir/$filename")
            mkdir -p "$file_dir"

            # Write file content
            echo -n "$content" > "$temp_dir/$filename"
            ;;
        --filename)
            if [ $# -lt 2 ]; then
                echo "Error: --filename requires a source file argument" >&2
                exit 1
            fi

            source_file="$2"
            shift 2

            # Check if source file exists
            if [ ! -f "$source_file" ]; then
                echo "Error: Source file '$source_file' not found" >&2
                exit 1
            fi

            # Use basename as the target filename in VEB
            target_filename=$(basename "$source_file")

            # Validate target filename (no path traversal)
            if [[ "$target_filename" == "." || "$target_filename" == ".." ]]; then
                echo "Error: Invalid target filename '$target_filename'" >&2
                exit 1
            fi

            # Copy file to temp_dir
            cp "$source_file" "$temp_dir/$target_filename"
            ;;
        --symlink)
            if [ $# -lt 3 ]; then
                echo "Error: --symlink requires link_name and target arguments" >&2
                exit 1
            fi

            link_name="$2"
            link_target="$3"
            shift 3

            # Create parent directory if needed
            link_dir=$(dirname "$temp_dir/$link_name")
            mkdir -p "$link_dir"

            # Create symlink in temp_dir
            # Note: This is for testing, including creating malicious symlinks
            (cd "$temp_dir" && ln -s "$link_target" "$link_name")
            ;;
        --hardlink)
            if [ $# -lt 3 ]; then
                echo "Error: --hardlink requires link_name and target arguments" >&2
                exit 1
            fi

            link_name="$2"
            link_target="$3"
            shift 3

            # Create parent directory if needed
            link_dir=$(dirname "$temp_dir/$link_name")
            mkdir -p "$link_dir"

            # Create target file if it doesn't exist (hardlinks need existing target)
            target_dir=$(dirname "$temp_dir/$link_target")
            mkdir -p "$target_dir"
            touch "$temp_dir/$link_target"

            # Create hardlink in temp_dir
            (cd "$temp_dir" && ln "$link_target" "$link_name")
            ;;
        *)
            echo "Error: Unknown option '$1'" >&2
            echo "Use --file, --filename, --symlink, or --hardlink" >&2
            exit 1
            ;;
    esac
done

# Create the VEB archive (regular tar, not gzipped) with files at root level
output_file="${extension_name}.veb"
current_dir="$MYSQL_TMP_DIR"
cd "$temp_dir"
tar -cf "$output_file" *

# Move to current directory
mv "$output_file" "$current_dir/$output_file"

echo "Created $output_file with contents:"
tar -tf "$current_dir/$output_file" | sed 's/^/  /' | sort
