# Skip test on x86 release builds - floating-point results differ there
# Debug builds, and release builds on other architectures, still run the test.
#
# Use this file when an optimized x86 build computes a different result than
# debug builds and other architectures, e.g. a geometry library dropping or
# keeping a ring depending on the last bits of an intersection point.

let $vsql_is_debug = `SELECT VERSION() LIKE '%debug%'`;
let $vsql_is_x86 = `SELECT @@version_compile_machine IN ('x86_64', 'AMD64', 'x86', 'i686')`;
if (!$vsql_is_debug)
{
  if ($vsql_is_x86)
  {
    --skip Result differs on x86 release builds - runs on debug and non-x86 builds
  }
}
