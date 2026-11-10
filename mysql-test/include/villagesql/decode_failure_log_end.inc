# Remove the saved error-log position file written by
# decode_failure_log_init.inc.
#
# Usage:
#   --source include/villagesql/decode_failure_log_init.inc
#   ... trigger decode failure ...
#   --source include/villagesql/decode_failure_log_print.inc
#   ... cleanup ...
#   --source include/villagesql/decode_failure_log_end.inc

--perl
use strict;
my $pos_file = $ENV{MYSQLTEST_VARDIR} . "/tmp/decode_fail_log_pos.txt";
unlink($pos_file);
EOF
