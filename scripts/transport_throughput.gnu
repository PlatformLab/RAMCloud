###
### WARNING: Requires gnuplot 4.4
###

set terminal postscript eps enhanced 'NimbusSanL-Regu' 14 color
set output "run/transport_throughput.eps"
set title "Transport Throughput"

set style line 1 lt 1 lw 4 pt 1 linecolor rgb "#E41A1C"
set style line 2 lt 2 lw 4 pt 2 linecolor rgb "#377EB8"
set style line 3 lt 3 lw 4 pt 3 linecolor rgb "#4DAF4A"
set style line 4 lt 4 lw 4 pt 4 linecolor rgb "#984EA3"
set style line 5 lt 5 lw 4 pt 5 linecolor rgb "#FF7F00"
set style line 6 lt 6 lw 4 pt 6 linecolor rgb "#FFFF33"
set style line 7 lt 7 lw 4 pt 7 linecolor rgb "#A65628"
set style line 8 lt 8 lw 4 pt 8 linecolor rgb "#F781BF"

set key bottom right
set grid

set xlabel "Object Size (Bytes)"
#set logscale x 2
set xrange [0:1048576]

set ylabel "Throughput (MB/s)"
set yrange [0:2000]
set ytics 200
set mytics 2

plot "run/transport_throughput.data" . \
     "" index "infrc"  using 1:2 with linespoints ls 3 title "infrc", \
     "" index "tcp" using 1:2 with linespoints ls 4 title "tcp", \
     "" index "unreliable+infud" using 1:2 with linespoints ls 2 title "unreliable+infud", \
     "" index "fast+infud" using 1:2 with linespoints ls 1 title "fast+infud", \
     "" index "unreliable+infeth" using 1:2 with linespoints ls 7 title "unreliable+infeth", \
     "" index "fast+infeth" using 1:2 with linespoints ls 5 title "fast+infeth", \
     "" index "unreliable+udp" using 1:2 with linespoints ls 6 title "unreliable+udp", \
     "" index "fast+udp"  using 1:2 with linespoints ls 8 title "fast+udp", \
     0 notitle
