#!/usr/bin/env perl

use strict;
use warnings;
use Data::Dumper;
use Getopt::Long;
use Storable;

my ($coordinatorLocator, $benchBinary, $dump, $transport, $help, $debug);
$dump = "latencyVSload.dump";
my $result = GetOptions ("coordinatorLocator=s" => \$coordinatorLocator,
                         "benchBinary=s"   => \$benchBinary,
			 "transport=s" => \$transport,
                         "debug" => \$debug,
			 "dump" => \$dump,
                         "help"  => \$help);


if (defined $help ||
    !defined $coordinatorLocator ||
    !defined $benchBinary
   ) {
	print "Usage: $0 --coordinatorLocator locator-string --benchBinary binary [--dump <Dumpfile for numbers> --help --debug]\n";
	exit 1;
}

my %data;
my $size = 1000;
my $operation_count = 10000;

my @loads = ( 1 .. 5 );

foreach my $clients (@loads) {
	for (my $i=0; $i<$clients; $i++) {
		system("$benchBinary -C $coordinatorLocator ".
		       "-t test.$i -R -m -S $size -n $operation_count > $i.log &");
	}
	
	print STDERR "Setting up a pre-existing load of $clients clients.\n";
	# run measurement 1/10 number of times so that it finishes
	# first.
	my $measure_operation_count = $operation_count/10;
	open (B, "$benchBinary -C $coordinatorLocator ".
	      "-t test.size$clients -R -m -S $size -n $measure_operation_count |") 
		or die "Cannot open binary - $!";
	while (<B>) {
		chomp;
		next if (m/^Reads:/);
		next if (m/^client:/);
		my ($op, $units, $number) = split /\s+/;
		$data{$clients}->{$op."-".$units} = $number;
	};
	close (B) or 
		die "Close failed - $!";

	# kill everything before starting next thingy
	system("pkill Bench");
}

print Dumper \%data;
store \%data, $dump;

exit 0;
