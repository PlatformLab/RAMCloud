#!/usr/bin/env perl

use strict;
use warnings;
use Data::Dumper;
use Getopt::Long;
use Storable;

my ($coordinatorLocator, $dump, $benchBinary, $transport, $help, $debug);
my $result = GetOptions ("coordinatorLocator=s" => \$coordinatorLocator,
                         "benchBinary=s"   => \$benchBinary,
                         "transport=s"   => \$transport,
                         "dump=s"   => \$dump,
                         "debug" => \$debug,
                         "help"  => \$help);


if (defined $help ||
    !defined $coordinatorLocator ||
    !defined $benchBinary ||
    !defined $dump
   ) {
	print "Usage: $0 --coordinatorLocator locator-string --benchBinary binary --dump <file-to-dump-data> [--transport <transport name>] [--help] --debug]\n";
  print "          [--clienthost <host> - optional host to run client on, instead of default host - same as master.\n";
	exit 1;
}

my %data;
my @sizes;
push @sizes, ($_ * 10) for (1, 10, 100);
push @sizes, ($_ * 10) for (1 .. 9);
push @sizes, ($_ * 100) for (1 .. 9);
# push @sizes, ($_ * 1000) for (1 .. 9);
# push @sizes, ($_ * 10000) for (1 .. 9);
# push @sizes, ($_ * 100000) for (1 .. 9);

for (my $i=0; $i<2; $i++) {
	system("$benchBinary -C $coordinatorLocator ".
	       "-t test.$i -R -m -S 1000 -n 100000 > $i.log &");
}

for my $size (@sizes) {
	open (B, "$benchBinary -C $coordinatorLocator ".
	      "-t test.size$size -R -m -S $size -n 100000 |") 
		or die "Cannot open binary - $!";
	while (<B>) {
		chomp;
		next if (m/^Reads:/);
		next if (m/^client:/);
		my ($op, $units, $number) = split /\s+/;
		$data{$size}->{$op."-".$units} = $number;
	};
	close (B) or 
		die "Close failed - $!";
}

$Data::Dumper::Sortkeys = 1;
print Dumper \%data;
store \%data, $dump;

exit 0;
