set terminal svg size 640, 320 fsize 10
set output "nightly.svg"
set yrange [0:300000]
set ylabel "Cycles/Read"
set xlabel "Date"
set xdata time  
set timefmt "%Y-%m-%d"  
set xrange ["2010-06-01":"2010-06-30"]  
set format x "%m/%d"  
set timefmt "%Y-%m-%d"  
set title "Nightly Read Performance"
plot \
    "nightly.log" using 1:3 title "Cycles/Read"
