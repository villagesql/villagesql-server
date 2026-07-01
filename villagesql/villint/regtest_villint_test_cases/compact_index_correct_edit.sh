#!/bin/bash
# Edit script for compact_index_correct test case
# Creates a C++ file that correctly uses sqlcom_compact_index()
# when indexing com_stat with a VSQL enum value

set -e

# The test clone may not have the VSQL enum block, so patch the header
# to include it (matching the real enum structure).
HEADER="include/my_sqlcommand.h"
if ! grep -q 'SQLCOM_VSQL_FIRST' "$HEADER"; then
  sed -i.bak 's/  SQLCOM_INSTALL_EXTENSION,/  SQLCOM_VSQL_FIRST = 1024,\n  SQLCOM_INSTALL_EXTENSION = SQLCOM_VSQL_FIRST,/' "$HEADER"
  rm -f "$HEADER.bak"
  git add "$HEADER"
fi

FILE="sql/test_compact_index_ok.cc"

echo "Creating file with correct VSQL com_stat access: $FILE"

cat > "$FILE" << 'EOF'
/* Copyright (c) 2026 VillageSQL Contributors */
#include "my_sqlcommand.h"
#include "sql/system_variables.h"

void good_counter(System_status_var *sv) {
  sv->com_stat[sqlcom_compact_index(SQLCOM_INSTALL_EXTENSION)]++;
}
EOF

git add "$FILE"

echo "Done: $FILE created with correct VSQL com_stat access"
