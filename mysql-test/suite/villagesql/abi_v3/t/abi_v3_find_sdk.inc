# Locates VillageSQL SDK paths for ABI v3 compatibility tests.
#
# Reads villagesql_test_env.sh from the build directory (available via the
# MYSQLD_BASEDIR environment variable set by mysql-test-run) and writes the
# resolved paths to files in $MYSQLTEST_VARDIR/tmp/ for use in --exec commands:
#
#   vsql_source_dir.txt      - VillageSQL source root (for cmake config lookup)
#   vsql_stable_sdk.txt      - stable_sdk/v3/ path (for binary compat builds)
#   vsql_dev_sdk_include.txt - current dev SDK include-dev/ (for src compat builds)
#
# Caller must --exec mkdir -p $MYSQLTEST_VARDIR/tmp before sourcing this file.

--perl
use strict;

use File::Basename qw(dirname);

my $vardir = $ENV{'MYSQLTEST_VARDIR'} or die "MYSQLTEST_VARDIR not set";
# Walk up from MYSQLTEST_VARDIR to locate villagesql_test_env.sh in the build
# root.  In non-parallel runs VARDIR is $BUILD/mysql-test/var (2 levels up);
# in parallel runs each worker gets $BUILD/mysql-test/var/N (3 levels up), so
# we search rather than assuming a fixed depth.
my $env_file;
my $dir = $vardir;
while (1) {
    my $parent = dirname($dir);
    last if $parent eq $dir;  # reached filesystem root without finding the file
    $dir = $parent;
    my $candidate = "$dir/villagesql_test_env.sh";
    if (-f $candidate) {
        $env_file = $candidate;
        last;
    }
}

unless (defined $env_file) {
    print "villagesql_test_env.sh not found above: $vardir\n";
    print "Re-run cmake to regenerate.\n";
    exit(1);
}

my %sdk;
open(my $fh, '<', $env_file) or die "Cannot read $env_file: $!";
while (<$fh>) {
    chomp;
    $sdk{$1} = $2 if /^(\w+)=(.+)$/;
}
close $fh;

for my $key (qw(VILLAGESQL_SOURCE_DIR VILLAGESQL_STABLE_SDK VILLAGESQL_DEV_SDK_INCLUDE)) {
    unless (defined $sdk{$key} && -e $sdk{$key}) {
        print "$key not found: " . ($sdk{$key} // "(not set)") . "\n";
        exit(1);
    }
}

my %out_files = (
    'vsql_source_dir.txt'      => $sdk{'VILLAGESQL_SOURCE_DIR'},
    'vsql_stable_sdk.txt'      => $sdk{'VILLAGESQL_STABLE_SDK'},
    'vsql_dev_sdk_include.txt' => $sdk{'VILLAGESQL_DEV_SDK_INCLUDE'},
);
for my $name (keys %out_files) {
    open(my $out, '>', "$vardir/tmp/$name") or die "Cannot write $name: $!";
    print $out $out_files{$name};
    close $out;
}

print "Found VillageSQL SDK paths\n";
EOF
