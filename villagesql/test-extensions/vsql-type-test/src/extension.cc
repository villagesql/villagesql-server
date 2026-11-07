#include <villagesql/vsql.h>
#include <string>
#include <string_view>

static constexpr const char kComplexTypeName[] = "COMPLEX";

void complex_from_string(std::string_view from, vsql::CustomResult out) {}

void complex_to_string(vsql::CustomArg in, vsql::StringResult out) {}

int complex_compare(vsql::CustomArg a, vsql::CustomArg b) { return 0; }

constexpr auto COMPLEX = vsql::make_type<kComplexTypeName>()
                             .persisted_length(16)
                             .max_decode_buffer_length(64)
                             .from_string<&complex_from_string>()
                             .to_string<&complex_to_string>()
                             .compare<&complex_compare>()
                             .intrinsic_default_str("(0, 0).")
                             .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(COMPLEX));
