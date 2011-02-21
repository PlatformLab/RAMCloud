#!/usr/bin/env perl

use strict;
use warnings;
use Data::Dumper;
use Getopt::Long;

use lib "scripts";
use HostPattern;

my ($help, @hostspattern);
my $default_pattern = "rc0[1-6].scs.stanford.edu,10.0.0.[1-6]";
push(@hostspattern, $default_pattern);

my $result = GetOptions ("hosts=s@" => \@hostspattern,
                         "help"  => \$help);

if (defined $help) {
  print <<"USAGE";
Usage: $0 [--help] [--hosts <>]

[--hosts <pattern for hostnames> - default is $default_pattern]
[--help] - this message

# This script runs a set of ssh connections in master mode in the
# background. Other ssh commands started after this script will be
# able to reuse these existing connections. Each user will need one of
# these on their own. Also passphraseless ssh should be set up and
# working before this script is executed.
#
# Make sure to add the following to your ~/.ssh/config so that ssh
# clients can connect using existing connections.

Host *
ControlMaster auto
ControlPath ~/.ssh/cm_socket/%r@%h:%p

# If master mode connections are already active, they are killed and
# new ones are established.
# 
# To confirm that master mode is available and being used run your ssh
# commands with the -v option and look for the following line.
#
#      debug1: auto-mux: Trying existing master
#

USAGE
  exit 1;
}

my $hosts = HostPattern::hosts(\@hostspattern);
print STDERR join ("\n", @$hosts)."\n";


foreach my $h (@$hosts) {
  my $cmd = "ssh -f -MNn $h";
  my $kill_cmd = "pkill -f \'$cmd\'";
  print STDERR "Calling $kill_cmd\n";
  system($kill_cmd);
  print STDERR "Calling $cmd\n";
  system($cmd) == 0
    or die "$cmd failed: $?";
}
