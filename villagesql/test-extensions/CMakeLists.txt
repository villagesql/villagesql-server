# Copyright (c) 2026 VillageSQL Contributors
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# Test extensions that use preview capabilities.
# Each line adds an ExternalProject that builds via the shared CMakeLists.txt
# template in test-extensions/shared/, so no per-extension CMakeLists.txt
# is needed.
#
# To add a new test extension:
#   1. Create villagesql/test-extensions/<dir-name>/src/extension.cc
#   2. Create villagesql/test-extensions/<dir-name>/manifest.json
#   3. Add a vsql_add_test_extension() line below.

vsql_add_test_extension(vsql-preview-ping-test         vsql_preview_ping_test)
vsql_add_test_extension(vsql-preview-ping-alt-test     vsql_preview_ping_alt_test)
vsql_add_test_extension(vsql-preview-unknown-cap-test  vsql_preview_unknown_cap_test)
vsql_add_test_extension(vsql-keyring-reader            vsql_keyring_reader)
vsql_add_test_extension(vsql-auth-test                 vsql_auth_test)
vsql_add_test_extension(vsql-lifecycle-test            vsql_lifecycle_test)
vsql_add_test_extension(vsql-thread-worker-test        vsql_thread_worker_test)
vsql_add_test_extension(vsql-preview-ping-no-cap-test  vsql_preview_ping_no_cap_test)
vsql_add_test_extension(vsql-preview-ping-v2-test      vsql_preview_ping_v2_test)
vsql_add_test_extension(vsql-sys-var-rollback-test     vsql_sys_var_rollback_test)
vsql_add_test_extension(vsql-noop-test                 vsql_noop_test)

# Built against the frozen stable v3 SDK so the variable-length type registers
# at VEF_PROTOCOL_3 and is rejected by build_type_descriptor_v3 (missing
# resolve_params) -- a path the current dev SDK can no longer produce.
vsql_add_test_extension(vsql-bad-var-no-resolve-test   vsql_bad_var_no_resolve_test  ABI v3)
vsql_add_test_extension(vsql-bitfield-test             vsql_bitfield_test)
vsql_add_test_extension(vsql-tarray-test               vsql_tarray_test)
vsql_add_test_extension(vsql-resolve-only-test         vsql_resolve_only_test)

# update_test installable + three target variants exercise the pre-checks
# of the extension version-update path. The VEB filename is
# update_test-<version>.veb; the manifest inside each variant carries the
# matching version string so the manifest/filename check passes.
vsql_add_test_extension(update-test-v1                 update_test  VERSION 1.0.0)
vsql_add_test_extension(update-test-v1.1               update_test  VERSION 1.1.0)
vsql_add_test_extension(update-test-v1.2               update_test  VERSION 1.2.0)
vsql_add_test_extension(update-test-v2                 update_test  VERSION 2.0.0)

# update_schema_test v1/v2 exercise on-disk representation migration across
# ALTER EXTENSION ... AT RESTART: v1 registers MYTYPE (4-byte int), v2 keeps
# MYTYPE (backward-compatible) and adds MYTYPE_V2 (8-byte int) plus a
# mytype_to_v2 conversion VDF.
vsql_add_test_extension(update-schema-test-v1          update_schema_test  VERSION 1.0.0)
vsql_add_test_extension(update-schema-test-v2          update_schema_test  VERSION 2.0.0)
