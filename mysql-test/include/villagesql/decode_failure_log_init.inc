# Record the current end-of-file position in the MySQL error log so that
# subsequent calls to decode_failure_log_print.inc only show entries written
# after this point.
#
# Usage:
#   --source include/villagesql/decode_failure_log_init.inc
#   ... trigger decode failure ...
#   --source include/villagesql/decode_failure_log_print.inc
#   ... cleanup ...
#   --remove_file $MYSQLTEST_VARDIR/tmp/decode_fail_log_pos.txt

--perl
use strict;
my $log = $ENV{MYSQLTEST_VARDIR} . "/log/mysqld.1.err";
my $pos_file = $ENV{MYSQLTEST_VARDIR} . "/tmp/decode_fail_log_pos.txt";
open(my $fh, "<", $log) or die "Cannot open $log: $!";
seek($fh, 0, 2);
my $pos = tell($fh);
close $fh;
open(my $pf, ">", $pos_file) or die "Cannot write $pos_file: $!";
print $pf $pos;
close $pf;
EOF
