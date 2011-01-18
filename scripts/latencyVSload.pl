#!/usr/bin/env perl

# This script runs a test that measures response times of ramcloud
# operations (latency) versus increasing loads.  This will attempt to
# execute a client on remote nodes and requires passphraseless ssh
# attempts to be working in order to run.  It also assumes that the
# source code is in someone's home directory on NFS so that it can
# find code on remote machines easily.  
# 
# Warning: Designed to run only one of these tests at a time. Will
# pkill the Bench binary to clean up after the test.

use strict;
use warnings;
use Data::Dumper;
use Getopt::Long;
use Storable;
use Cwd 'abs_path';

use lib "scripts";
use HostPattern;

my ($coordinatorLocator, $benchBinary, $dump, $transport, $help,
    $debug, @clientspattern);
$dump = "latencyVSload.dump";

my $result = GetOptions ("coordinatorLocator=s" => \$coordinatorLocator,
                         "benchBinary=s"   => \$benchBinary,
                         "transport=s" => \$transport,
                         "debug" => \$debug,
                         "dump=s" => \$dump,
                         "clienthosts=s@" => \@clientspattern,
                         "help"  => \$help);



if (defined $help ||
    !defined $coordinatorLocator ||
    !defined $benchBinary
   ) {
  print <<"USAGE";
Usage: $0 --coordinatorLocator locator-string
          --benchBinary binary 
          [--dump <Dumpfile for numbers>] 
          [--clienthosts <pattern for hostnames> - 
               default is no remote clients
               example - rc0[1-6].scs.stanford.edu]
          [--help]
          [--debug]
USAGE
  exit 1;
}

my %data;
my $size = 1000;
my $operation_count = 10000;

my @loads = ( 0 .. 15 );
my $clienthosts = HostPattern::hosts(\@clientspattern);
print STDERR join ("\n", @$clienthosts)."\n";

# Assume remote code is in the same place as current machine.
my $benchBinaryFull = abs_path($benchBinary);


foreach my $clients (@loads) {
  print STDERR "Setting up a pre-existing load of $clients clients.\n";
  for (my $i=0; $i<$clients; $i++) {
    # Reserve 0th host for measurement gathering.
    my $remotehost = $clienthosts->
      [($i % ((scalar @$clienthosts) - 1)) + 1]; 
    my $cmd =
      "ssh $remotehost \"$benchBinaryFull -C $coordinatorLocator ".
        "-t test.$i -R -m -S $size -n $operation_count 2>&1\" &";

    print STDERR "Load $clients : Number $i : Host $remotehost\n";
    system($cmd) == 0
      or die "$cmd failed: $?"; 
  }
 
  # run measurement 1/10 number of times so that it finishes
  # first.
  my $measure_operation_count = $operation_count/10;
  # Run on the first client host for measurements
  my $remotehost = $clienthosts->[0]; 

  open (B, "ssh $remotehost $benchBinaryFull -C $coordinatorLocator ".
        "-t test.size$clients -R -m -S $size -n $measure_operation_count |") 
    or die "Cannot open binary - $!";
  while (<B>) {
    chomp;
    next if (m/^Reads:/);
    next if (m/^client:/);
    my ($op, $units, $number) = split /\s+/;
    $data{$clients}->{$op."-".$units} = $number;
  }

  close (B) or 
    die "Close failed - $!";

  # kill all instances of Bench just in case.
  #TODO track pids and kill instead of name
  for (my $i=0; $i<$clients; $i++) {
    my $remotehost = $clienthosts->[$i % (scalar @$clienthosts)]; 
    system("ssh $remotehost pkill Bench");
  }
}

print STDERR Dumper \%data;
store \%data, $dump;
print STDERR "Finished running latency vs load test! Output is in $dump.\n";
exit 0;
