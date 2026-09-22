/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

#ifndef VILLAGESQL_CLONE_VSQL_CLONE_PROTOCOL_H_
#define VILLAGESQL_CLONE_VSQL_CLONE_PROTOCOL_H_

#include <algorithm>
#include <cstdint>

// VillageSQL clone protocol constants, kept out of the upstream clone plugin
// headers. The clone plugin includes this to encode/decode the VillageSQL
// clone version it negotiates alongside (but independently of) the upstream
// clone protocol version.
//
// TODO(villagesql-rebase): VillageSQL runs its own monotonic clone protocol
// version, negotiated in reserved high bits of the clone protocol-version word,
// kept away from upstream's version numbers. As of the 8.4 base upstream uses
// only the low 16 bits for the version (highest is 0x0103) and sets no high bit
// in that word (bit 31 is NO_BACKUP_LOCK_FLAG, but on the separate ddl_timeout
// word); we use bits 24-30 and leave 31 alone. The recipient encodes its
// VillageSQL clone version there; the donor masks those bits off before the
// upstream version comparison and negotiates the VillageSQL version separately
// as min(recipient, donor). This relies on upstream keeping the version in the
// low bits — re-check on each rebase.
//
// The VillageSQL clone version is independent of both the upstream clone
// protocol version and the extension-payload format
// (CLONE_EXTENSION_PAYLOAD_FORMAT in veb_file.h); bump it when the vsql<->vsql
// clone handshake/behavior changes.

namespace villagesql {
namespace clone {

// Bit position of the VillageSQL clone version field in the version word.
const uint32_t VSQL_CLONE_VERSION_SHIFT = 24;

// Mask for the VillageSQL clone version field (bits 24-30; bit 31 avoided).
const uint32_t VSQL_CLONE_VERSION_MASK = 0x7FU << VSQL_CLONE_VERSION_SHIFT;

// VillageSQL clone protocol versions. 0 means "not a VillageSQL clone" (e.g. a
// vanilla recipient, which sets no high bits).
const uint32_t VSQL_CLONE_VERSION_NONE = 0;

// v1: recipient validates donor VillageSQL extensions during the handshake.
const uint32_t VSQL_CLONE_VERSION_V1 = 1;

// Latest VillageSQL clone version this build supports.
const uint32_t VSQL_CLONE_VERSION = VSQL_CLONE_VERSION_V1;

// Encode a VillageSQL clone version into its reserved high-bit field.
inline uint32_t vsql_clone_version_bits(uint32_t vsql_version) {
  return (vsql_version << VSQL_CLONE_VERSION_SHIFT) & VSQL_CLONE_VERSION_MASK;
}

// Extract the VillageSQL clone version from a received version word.
inline uint32_t vsql_clone_version_of(uint32_t version_word) {
  return (version_word & VSQL_CLONE_VERSION_MASK) >> VSQL_CLONE_VERSION_SHIFT;
}

// Recipient side: build the clone version word to advertise, carrying this
// build's VillageSQL clone version in the reserved high bits alongside the
// given upstream clone protocol version. Returns the version word to put on the
// wire.
inline uint32_t encode_version_word(uint32_t upstream_version) {
  return upstream_version | vsql_clone_version_bits(VSQL_CLONE_VERSION);
}

// Donor side: split a received clone version word into the negotiated upstream
// protocol version and the negotiated VillageSQL clone version. The VillageSQL
// bits are masked off before the upstream version is clamped, so they never
// perturb upstream's comparison; the VillageSQL version is negotiated
// separately as min(recipient, this build). out_upstream_version and
// out_vsql_version receive the two negotiated values.
inline void negotiate_version_word(uint32_t recipient_word,
                                   uint32_t donor_upstream_version,
                                   uint32_t *out_upstream_version,
                                   uint32_t *out_vsql_version) {
  uint32_t upstream = recipient_word & ~VSQL_CLONE_VERSION_MASK;
  if (upstream > donor_upstream_version) {
    upstream = donor_upstream_version;
  }
  *out_upstream_version = upstream;

  const uint32_t recipient_vsql = vsql_clone_version_of(recipient_word);
  *out_vsql_version = std::min(recipient_vsql, VSQL_CLONE_VERSION);
}

}  // namespace clone
}  // namespace villagesql

#endif  // VILLAGESQL_CLONE_VSQL_CLONE_PROTOCOL_H_
