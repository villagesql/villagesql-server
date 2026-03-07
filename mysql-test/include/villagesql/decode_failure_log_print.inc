# Print decode-failure log entries written since the last call to
# decode_failure_log_init.inc or this file, then advance the saved position.
# Matches lines containing "Invalid value in column" or
# "Extension error decoding column", stripping the timestamp/thread prefix.
#
# Usage:
#   --source include/villagesql/decode_failure_log_init.inc
#   ... trigger decode failure ...
#   --source include/villagesql/decode_failure_log_print.inc

--perl
use strict;
my $log = $ENV{MYSQLTEST_VARDIR} . "/log/mysqld.1.err";
my $pos_file = $ENV{MYSQLTEST_VARDIR} . "/tmp/decode_fail_log_pos.txt";
open(my $pf, "<", $pos_file) or die "Cannot open $pos_file: $!";
my $pos = <$pf>; chomp $pos; close $pf;
open(my $fh, "<", $log) or die "Cannot open $log: $!";
seek($fh, $pos, 0);
while (my $line = <$fh>) {
  next unless $line =~ /Invalid value in column|Extension error decoding column/;
  $line =~ s/^\S+\s+\d+\s+\[Warning\]\s+\[MY-\d+\]\s+\[Server\]\s+//;
  print $line;
}
my $new_pos = tell($fh); close $fh;
open($pf, ">", $pos_file) or die "Cannot write $pos_file: $!";
print $pf $new_pos; close $pf;
EOF
