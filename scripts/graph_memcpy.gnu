set terminal png size 960, 1880
set output "memcpy.png"

set multiplot layout 2, 1 title "memcpy performance"
set grid

set xlabel "bytes copied"
set logscale x 2
set format x "2^%L"

set logscale y 10

set pointsize 0.01

set title "absolute performance"
set ylabel "nsec"
set format y "10^%L"
set key left top
load "absolute.gnu"
unset format
set format x "2^%L"

set title "bandwidth"
set ylabel "GB/s"
set key right top
unset logscale y
load "bandwidth.gnu"
unset multiplot
