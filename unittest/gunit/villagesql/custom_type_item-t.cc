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

#include <gtest/gtest.h>

#include "sql/item.h"
#include "unittest/gunit/fake_table.h"
#include "unittest/gunit/test_utils.h"
#include "villagesql/schema/descriptor/type_context.h"

namespace villagesql_unittest {

class CustomTypeItemTest : public ::testing::Test {
 protected:
  void SetUp() override { initializer.SetUp(); }
  void TearDown() override { initializer.TearDown(); }

  my_testing::Server_initializer initializer;
};

// An ASCII literal can encode to arbitrary bytes when assigned to a custom
// column. Exercise the actual encoding and field-store path, including the
// metadata on the rewritten literal.
TEST_F(CustomTypeItemTest, CustomTypeEncodingUpdatesLiteralRepertoire) {
  auto encode = +[](unsigned char *out, size_t capacity, const char *, size_t,
                    size_t *written) -> bool {
    if (capacity < 2) return true;
    out[0] = 0xAB;
    out[1] = 0xCD;
    *written = 2;
    return false;
  };
  auto decode = +[](const unsigned char *, size_t, char *, size_t,
                    size_t *) -> bool { return false; };
  auto compare = +[](const unsigned char *, size_t, const unsigned char *,
                     size_t) -> int { return 0; };
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("BINARY_PAIR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_1, 1, 2, 16, /*max_persisted_length=*/0,
      villagesql::LengthKind::Fixed, villagesql::EncodeFunction(encode),
      villagesql::DecodeFunction(decode), villagesql::CompareFunction(compare));
  auto ctx = villagesql::TableTraits<villagesql::TypeContext>::create(
      villagesql::TypeContextKey("BINARY_PAIR", "test_ext", "1.0.0"), &desc);
  ASSERT_NE(ctx, nullptr);

  Fake_TABLE_SHARE share(1);
  Field_varstring field(nullptr, 2, 1, nullptr, 0, Field::NONE, "c", &share,
                        &my_charset_bin);
  Fake_TABLE table(&field);
  bitmap_set_bit(table.write_set, 0);
  field.set_type_context(ctx.get());

  auto *literal = new Item_string("ascii", 5, &my_charset_utf8mb4_0900_ai_ci,
                                  DERIVATION_COERCIBLE, MY_REPERTOIRE_ASCII);
  ASSERT_EQ(literal->collation.repertoire, MY_REPERTOIRE_ASCII);
  ASSERT_EQ(literal->save_in_field(&field, false), TYPE_OK);

  EXPECT_EQ(literal->get_type_context(), ctx.get());
  EXPECT_EQ(literal->collation.collation, &my_charset_bin);
  EXPECT_EQ(literal->collation.derivation, DERIVATION_COERCIBLE);
  EXPECT_EQ(literal->collation.repertoire, MY_REPERTOIRE_UNICODE30);
  String buffer;
  String *stored = field.val_str(&buffer, &buffer);
  ASSERT_NE(stored, nullptr);
  ASSERT_EQ(stored->length(), 2u);
  EXPECT_EQ(static_cast<unsigned char>(stored->ptr()[0]), 0xAB);
  EXPECT_EQ(static_cast<unsigned char>(stored->ptr()[1]), 0xCD);
}

}  // namespace villagesql_unittest
