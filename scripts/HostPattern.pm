package HostPattern;

# Provide a simple pattern reader for interpreting host patterns where
# these patterns contain numerical ranges.
# Eg:- [1-3] = 1 2 3
#      [4-7],11,13 = 4 5 6 7 11 13

use strict;
use warnings;


sub hosts {
  my (@hosts) = @{$_[0]};
  my @hostlist;

  while (@hosts) {
    my $h = shift(@hosts);
    if ($h =~ /^(.*?)\[([^\]]+)\]([^,{[]*)(,(.*))?$/) {
      my($pre,$post) = ($1,$3);
      my @r = split(/,/,$2);
      for (@r) {
        if (/^(!?)(\d+)(-(\d+))?$/) {
          my $low = $2;
          my $high = $3 ? $4 : $low;
          my $fmt = "%d";
          if (length($low) == length($high) && length($low) > 1) {
            $fmt = "%0" . length($low) . "d";
          }
          for (my $n = $low; $n <= $high; $n++) {
            my $hn = $pre . sprintf($fmt,$n) . $post;
            if ($1) {
              @hostlist = grep { $_ ne $hn } @hostlist;
              @hosts = grep { $_ ne $hn } @hosts;
            } else {
              push(@hosts,$hn);
            }
          }
        } else {
          die "Unrecognized host pattern: $h";
        }
      }
      if ($4) {
        push(@hosts,$5);
      }
    } elsif ($h =~ /^(.*?)\{([^\]]+)\}([^,[{]*)(,(.*))?$/) {
      my($pre,$post) = ($1,$3);
      my @r = split(/,/,$2);
      for (@r) {
        push(@hosts,$pre.$_.$post);
      }
      if ($4) {
        push(@hosts,$5);
      }
    } else {
      my @h = split(/,/,$h);
      push(@hostlist,@h);
    }
  }

  return \@hostlist;
}
           
1;
