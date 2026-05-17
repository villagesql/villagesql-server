use strict;
use warnings;

use IO::Socket::INET;

my ($port, $cmd) = @ARGV;

my $sock = IO::Socket::INET->new(
  PeerAddr => "127.0.0.1",
  PeerPort => $port,
  Proto    => "tcp",
  Timeout  => 5,
) or die "connect failed: $!";

$sock->autoflush(1);
print $sock "$cmd\n";
shutdown($sock, 1);
print do { local $/; <$sock> };
close($sock);
