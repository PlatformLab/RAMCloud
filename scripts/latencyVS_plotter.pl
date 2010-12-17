#!/usr/bin/env perl

use strict;
use warnings;
use Data::Dumper;
use Getopt::Long;
use Storable;

# Using this from home directory due to lack of centos package
# Install Chart::Gnuplot before running this machine.
use lib "$ENV{HOME}/lib/perl5/site_perl/5.8.8/";
use Chart::Gnuplot;

my ($dump, $help, $debug, $size);
my $transport = "";
my $result = GetOptions("dump=s" => \$dump,
			"transport=s"  => \$transport,
			"debug"  => \$debug,
			"size" =>\$size,
			"help"  => \$help);

if (defined $help ||
    !defined $dump
   ) {
  print "Usage: $0 --dump <file-with-dump-from-latencyVS*.pl> [--transport <transport>] [--size] [--debug] [--help]\n";
  print "--size - Plot object size instead of load as x-axis.\n";
  exit 1;
}

my $dataref = retrieve($dump);
my %data = %{$dataref};

print Dumper \%data;

sub make_coords {
  my ($key) = @_;
  my (@x, @y);
  foreach (sort {$a <=> $b} keys %data) {
    push @x, $_;
    push @y, $data{$_}->{$key};
    print "$_ ".$data{$_}->{$key}."\n" if $debug;
  }
  return [\@x, \@y, $key];
}

my $read_server = make_coords('readMany-avgctr');
my $read_client = make_coords('readMany-avgns');
my $write_server = make_coords('writeMany-avgctr');
my $write_client = make_coords('writeMany-avgns');

my $title = "Ramcloud clients / Response Time vs Load / ";
$title .= "Transport:$transport " if (defined $transport);
my $xlabel = "Load (number of existing clients)";
my $output_png = "latencyVSload.png";

if (defined $size) {
  $title =~ s/Response Time vs Load/Object Size vs Load/;
  $xlabel = "Size of Objects (bytes)";
  $output_png = "latencyVSsize.png";
}

my $chart = Chart::Gnuplot->
  new(
      output => $output_png,
      title => $title,
      xlabel => $xlabel,
      ylabel => "Avg time per operation (ns)",
      legend => {
		 align => "right",
		 position => "outside",
		 width => 3,
		 height => 4,
		 order => "vertical",
		}
     );

my @datasets;
foreach my $op($read_server,
	       $read_client,
	       $write_server,
	       $write_client) {
  push @datasets, Chart::Gnuplot::DataSet->
    new(
	xdata => $op->[0],
	ydata => $op->[1],
	style => "linespoints",
	title => $op->[2],
       );
}
$chart->plot2d(@datasets);

exit 0;
