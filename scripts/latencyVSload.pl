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

my ($coordinatorLocator, $benchBinary, $dump, $help,
    $multiobject, $debug, @clientspattern, $loadStart, $loadEnd, $size);

$dump = "latencyVSload.dump";
$loadStart = 6;
$loadEnd = 6;
$size = 100;

my $result = GetOptions ("coordinatorLocator=s" => \$coordinatorLocator,
                         "benchBinary=s"   => \$benchBinary,
                         "loadStart=s" => \$loadStart,
                         "loadEnd=s" => \$loadEnd,
                         "objSize=s" => \$size,
                         "multiobject" => \$multiobject,
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
          [--multiobject (Default is a single object written to and
                          read from.)]
          [--objSize - size of objects for RC operations in
                       bytes. Default 100 bytes.]
          [--loadStart - worker load to start benchmark with - default
0]
          [--loadEnd - worker load to end benchmark with - default 20]
          [--help]
          [--debug]
USAGE
  exit 1;
}

my %data;
# smallish count so that worker checks command flag responsively.
my $operation_count = 10000;


my @loads = ( $loadStart .. $loadEnd );
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
      "ssh $remotehost \"LOAD=$clients CLIENT=#$i $benchBinaryFull -C $coordinatorLocator ";
    # First client will write the object to bootstrap - everyone else
    # just reads
    if ($i != 0) {
      $cmd .= "-o ";
    }
    $cmd .= "-t test -S $size -n $operation_count --executionmode worker"
      ." --workerid $i 1>/tmp/worker.$i.log 2>&1\" &";
    # TODO multiobject option for writes 
    print STDERR "Calling $cmd\n";
    system($cmd) == 0
      or die "$cmd failed: $?";
    print STDERR "Load $clients : Number $i : Host $remotehost\n";
  }
 
  my $measure_operation_count = 100000;
  # Run on the first client host for measurements
  my $remotehost = $clienthosts->[0];

  my $cmd = "ssh $remotehost $benchBinaryFull -C $coordinatorLocator  "; 
  $cmd .= "--executionmode queen --numworkers $clients ";
  if ($clients != 0) {
    # Do the first bootstrapping write
    $cmd .= "-o ";
  }
  $cmd .= "-t test -S $size -n $measure_operation_count ";
  print STDERR "Calling $cmd\n";
  open (B, $cmd . " |") 
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
}

print STDERR Dumper \%data;
store \%data, $dump;
print STDERR "Finished running latency vs load test! Output is in $dump.\n";
exit 0;
