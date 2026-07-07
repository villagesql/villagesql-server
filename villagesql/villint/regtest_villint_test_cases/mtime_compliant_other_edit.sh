#!/bin/bash
# Edit script for mtime_compliant_other test case
# Creates a properly formatted non-C++ file (Python) that villint should not modify

set -e

FILE="scripts/villint_mtime_test.py"

echo "Creating compliant Python file: $FILE"

cat > "$FILE" << 'EOF'
#!/usr/bin/env python3
# Test file for mtime preservation

def properly_formatted_function():
    """This file has no trailing whitespace and ends with newline."""
    return 42
EOF

git add "$FILE"

echo "Done: Compliant Python file created"
