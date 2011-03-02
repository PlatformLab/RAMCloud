#!/usr/bin/env perl

use strict;
use warnings;
use Data::Dumper;
use Getopt::Long;
use Storable;
use File::Basename;

# Using this from home directory due to lack of centos package
# Install Chart::Gnuplot before running this machine.
use lib "$ENV{HOME}/lib/perl5/site_perl/5.8.8/";
use Chart::Gnuplot;

my ($dump, $help, $debug, $size);
my $params = "";
my $result = GetOptions("dump=s@" => \$dump,
			"params=s"  => \$params,
			"debug"  => \$debug,
			"size" =>\$size,
			"help"  => \$help);

if (defined $help ||
    !defined $dump
   ) {
  print "Usage: $0 --dump <file-with-dump-from-latencyVS*.pl> [--params <parameters in the experiment>] [--size] [--debug] [--help]\n";
  print "--size - Plot object size instead of load as x-axis.\n";
  exit 1;
}

sub make_coords {
  my ($key, $dataref) = @_;
  my %data = %$dataref;
  my (@x, @y);
  foreach (sort {$a <=> $b} keys %data) {
    push @x, $_;
    push @y, $data{$_}->{$key};
    print "$_ ".$data{$_}->{$key}."\n" if $debug;
  }
  return [\@x, \@y, $key];
}

# output file names are based on first dump's name
my $d1 = $dump->[0];
my $title = "Ramcloud clients / Latency vs Load / ";
$title .= "$params " if (defined $params);
my $title2 = "Ramcloud clients / Throughput vs Load / ";
$title2 .= "$params " if (defined $params);
my $xlabel = "Load (number of existing clients)";
my $output_png = basename($d1).".png";

if (defined $size) {
  $title =~ s/Load/Object Size/;
  $xlabel = "Size of Objects (bytes)";
  $output_png = basename($d1)."size.png";
}

my $chart = Chart::Gnuplot->
  new(
      imagesize => "2, 2",
      output => $output_png,
      title => { text => $title, font => "Helvetica, 20", },
      xlabel => $xlabel,
      ylabel => "Latency - Avg time per operation (ns)",
      legend => {
		 align => "right",
		 position => "outside",
		 width => 3,
		 height => 4,
		 order => "vertical",
		}
     );

my $chart2 = Chart::Gnuplot->
  new(
      imagesize => "2, 2",
      output => "tput".$output_png,
      title => { text => $title2, font => "Helvetica, 20", },
      xlabel => $xlabel,
      ylabel => "Throughput - Operations per second",
      legend => {
		 align => "right",
		 position => "outside",
		 width => 3,
		 height => 4,
		 order => "vertical",
		}
     );


my (@ldatasets, @tdatasets);

foreach my $d (@$dump) {
    my $dataref = retrieve($d);
    my %data = %{$dataref};

    print Dumper \%data;

    my $read_client = make_coords('readMany-avgns', $dataref);
    my $write_client = make_coords('writeMany-avgns', $dataref);
    my $read_tput = make_coords('tputread-ops/sec', $dataref);

    foreach my $op($read_client,
#		   $write_client,
	) {
	push @ldatasets, Chart::Gnuplot::DataSet->
	    new(
		xdata => $op->[0],
		ydata => $op->[1],
		style => "lines",
		title => $d."-".$op->[2],
	    );
    }
    
    foreach my $op($read_tput) {
	push @tdatasets, Chart::Gnuplot::DataSet->
	    new(
		xdata => $op->[0],
		ydata => $op->[1],
		style => "lines",
		title => $d."-".$op->[2],
	    );
    }
}
$chart->plot2d(@ldatasets);
$chart2->plot2d(@tdatasets);


exit 0;
