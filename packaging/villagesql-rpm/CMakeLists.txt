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

# Configures the VillageSQL RPM spec. Kept separate from packaging/rpm-oel so
# that upstream's spec stays untouched across MySQL rebases.

IF(NOT LINUX)
  RETURN()
ENDIF()

# RPM forbids '-' in Version, and orders '~' BEFORE the plain version, which is
# what a pre-release needs: 0.0.6~dev < 0.0.6. Using '-dev' or '.dev' would
# sort AFTER the eventual release and make 0.0.6 un-upgradable from 0.0.6-dev.
SET(VSQL_RPM_VERSION
  "${VSQL_MAJOR_VERSION}.${VSQL_MINOR_VERSION}.${VSQL_PATCH_VERSION}")
IF(VSQL_HAS_PRE_RELEASE)
  SET(VSQL_RPM_VERSION "${VSQL_RPM_VERSION}~${VSQL_PRE_RELEASE_VERSION}")
ENDIF()

# Name of the directory inside the source tarball. %setup -q -n expects the
# tarball to unpack to exactly this.
SET(VSQL_SRC_DIR "villagesql-server-${VSQL_RPM_VERSION}")

SET(SPECFILENAME "villagesql.spec")

CONFIGURE_FILE(villagesql.spec.in
  ${CMAKE_CURRENT_BINARY_DIR}/${SPECFILENAME} @ONLY)

CONFIGURE_FILE(villagesql.logrotate
  ${CMAKE_CURRENT_BINARY_DIR}/villagesql.logrotate COPYONLY)

MESSAGE(STATUS "VillageSQL RPM version: ${VSQL_RPM_VERSION}")
MESSAGE(STATUS "VillageSQL RPM spec: ${CMAKE_CURRENT_BINARY_DIR}/${SPECFILENAME}")
