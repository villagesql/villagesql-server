# This include file should be included at the end of all tests that uses
# NDB backup, it will delete the backup directory which mtr will
# otherwise report an error for.

if (!$NDB_BACKUP_DIR)
{
  --die Need NDB_BACKUP_DIR variable
}

--perl
use strict;
use lib "lib/";
use File::Path;
use locale ':not_characters';

my $backup_dir = $ENV{NDB_BACKUP_DIR}."/BACKUP" or die "Need NDB_BACKUP_DIR";

rmtree($backup_dir, 0, 1);

EOF

