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
#      yrange => [0, 15000],
#      y2range => [0, 1000*1000*1000],
#      y2tics => 1000*1000*100,
#     y2tics => 10,
      grid => {},
#      ylabel => "Avg Number of Buffers returned",
      ylabel => "Latency - Avg time per operation (ns)",
#      y2label => "# of operations counted",
      legend => {
		 align => "right",
		 position => "inside",
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
      yrange => [0, 1200000],
      grid => {},
      legend => {
		 align => "right",
		 position => "inside",
		 width => 3,
		 height => 4,
		 order => "vertical",
		}
     );


my (@ldatasets, @tdatasets);

my $i = 0;
foreach my $d (@$dump) {
#    last if ($i == 1 or $i == 2);
    last if $i != 0;
    my $dataref = retrieve($d);
    my %data = %{$dataref};
    $Data::Dumper::Sortkeys = 1;
    print Dumper \%data;

=pod
          '19' => {
                    'serverwait-avgns' => '346',
                    'serverread-avgns' => '73',
                    'gtbPollNanos-avgns' => '28',
                    'tputgettx-ops/sec' => '25733040',
                    'infrcGetTxMedian-ns' => '7',
                    'gtbPollCount-ct' => '31477',
                    'gtbPollZeroNCount-ct' => '0',
                    'infrcGetTxAvg-ns' => '37',
                    'gtbPollNonZeroNAvg-ct' => '1',
                    'readMany-ns' => '2069009937',
                    'infrcSendReplyNanos-avgns' => '250',
                    'readCount-ct' => '1998207',
                    'infrcGetTxMax-ns' => '7248',
                    'readMany-avgns' => '20690',
                    'tputread-ops/sec' => '965710',
                    'infrcGetTxBuffer-avgns' => '38',
                    'infrcGetTxZeroCount-ct' => '1952892',
                    'infrcGetTxCount-ct' => '1998577'
                  }

=cut

    my $read_client = make_coords('readMany-avgns', $dataref);
    my $write_client = make_coords('writeMany-avgns', $dataref);

    my $server_wait = make_coords('serverwait-avgns', $dataref);
    my $server_read = make_coords('serverread-avgns', $dataref);
    my $server_infreply = make_coords('infrcSendReplyNanos-avgns', $dataref);

    my $server_infbuf = make_coords('infrcGetTxBuffer-avgns', $dataref);
    my $server_infbufzc = make_coords('infrcGetTxZeroCount-ct', $dataref);
    my $server_infbufrc = make_coords('readCount-ct', $dataref);
    my $server_infbufct = make_coords('infrcGetTxCount-ct', $dataref);
    my $server_infbufmed = make_coords('infrcGetTxMedian-ns', $dataref);
    my $server_infbufmax = make_coords('infrcGetTxMax-ns', $dataref);
    my $server_infbufavg = make_coords('infrcGetTxAvg-ns', $dataref);

    my $server_gtbpollnanos = make_coords('gtbPollNanos-avgns', $dataref);
    my $server_gtbpolleffnanos = make_coords('gtbPollEffNanos-avgns', $dataref);
    my $server_gtbpollzeronanos = make_coords('gtbPollZeroNanos-avgns', $dataref);
    my $server_gtbpollnonzeronanos = make_coords('gtbPollNonZeroNanos-avgns', $dataref);
    my $server_gtbpoll = make_coords('gtbPollCount-ct', $dataref);
    my $server_gtbpollzeron = make_coords('gtbPollZeroNCount-ct', $dataref);
    my $server_gtbpollnavg = make_coords('gtbPollNonZeroNAvg-ct', $dataref);

    my $read_tput = make_coords('tputread-ops/sec', $dataref);
    my $server_infbuf_tput = make_coords('tputgettx-ops/sec', $dataref);


    foreach my $op(
#	           $read_client,
#		   $server_wait,
#		   $server_read,
		   $server_infbuf,
#		   $server_infbufmed,
#		   $server_infbufmax,
#		   $server_infbufavg,
#		   $server_gtbpollnanos,
		   $server_gtbpolleffnanos,
		   $server_gtbpollzeronanos,
		   $server_gtbpollnonzeronanos,
#		   $server_infreply,
#		   $write_client,
	) {
	push @ldatasets, Chart::Gnuplot::DataSet->
	    new(
		xdata => $op->[0],
		ydata => $op->[1],
		style => "linespoints ls ".(++$i % 7),
		title => $d."-".$op->[2],
	       );
      }
=pod
    foreach my $op(
#		   $server_infbufzc,
#		   $server_infbufrc,
#		   $server_infbufct,
#		   $server_gtbpoll,
#		   $server_gtbpollzeron,
		   $server_gtbpollnavg,
	) {
	push @ldatasets, Chart::Gnuplot::DataSet->
	    new(
		xdata => $op->[0],
		ydata => $op->[1],
		axes  => "x1y2",
		style => "lines ls ".(++$i % 7),
		title => $d."-".$op->[2],
	    );
      }
=cut
    foreach my $op($read_tput,
#		   $server_infbuf_tput,
	) {
	push @tdatasets, Chart::Gnuplot::DataSet->
	    new(
		xdata => $op->[0],
		ydata => $op->[1],
		style => "lines ls ".(++$i % 7),
		title => $d."-".$op->[2],
	    );
      }

  } # per dump


my $cmd = '
set style line 1 lt 1 lw 4 pt 1 linecolor rgb "#E41A1C"
set style line 2 lt 2 lw 4 pt 2 linecolor rgb "#377EB8"
set style line 3 lt 3 lw 4 pt 3 linecolor rgb "#4DAF4A"
set style line 4 lt 4 lw 4 pt 4 linecolor rgb "#984EA3"
set style line 5 lt 5 lw 4 pt 5 linecolor rgb "#FF7F00"
set style line 6 lt 6 lw 4 pt 6 linecolor rgb "#F781BF"
set style line 7 lt 7 lw 4 pt 7 linecolor rgb "#A65628"
';
$chart->command($cmd);
$chart2->command($cmd);

$chart->plot2d(@ldatasets);
$chart2->plot2d(@tdatasets);


exit 0;
