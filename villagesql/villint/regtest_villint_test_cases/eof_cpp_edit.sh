#!/bin/bash
# Edit script for eof_cpp test case
# Removes the trailing newline from sql/mysqld.cc

set -e

FILE="sql/mysqld.cc"

echo "Removing EOF newline from $FILE"

# Remove trailing newline using perl
# -i = in-place edit
# -pe = print each line, execute code
# This reads the file, removes the final newline if present
perl -pi -e 'chomp if eof' "$FILE"

echo "Done: $FILE now lacks EOF newline"
