#!/bin/bash
# Edit script for compact_index_missing test case
# Creates a C++ file that indexes com_stat with a VSQL enum value
# without using sqlcom_compact_index()

set -e

# The test clone may not have the VSQL enum block, so patch the header
# to include it (matching the real enum structure).
HEADER="include/my_sqlcommand.h"
if ! grep -q 'SQLCOM_VSQL_FIRST' "$HEADER"; then
  sed -i.bak 's/  SQLCOM_INSTALL_EXTENSION,/  SQLCOM_VSQL_FIRST = 1024,\n  SQLCOM_INSTALL_EXTENSION = SQLCOM_VSQL_FIRST,/' "$HEADER"
  rm -f "$HEADER.bak"
  git add "$HEADER"
fi

FILE="sql/test_compact_index.cc"

echo "Creating file with raw VSQL com_stat access: $FILE"

cat > "$FILE" << 'EOF'
/* Copyright (c) 2026 VillageSQL Contributors */
#include "my_sqlcommand.h"
#include "sql/system_variables.h"

void bad_counter(System_status_var *sv) {
  sv->com_stat[SQLCOM_INSTALL_EXTENSION]++;
}
EOF

git add "$FILE"

echo "Done: $FILE created with raw VSQL com_stat access"
