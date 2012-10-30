# ReplicatedSegment::LOG_RECOVERY_REPLICATION_RPC_TIMING. Used to visualize
# log replication progress during recovery using log messages.

set terminal png
set output "recovery-rpc-timeline.png"
set title "Acknowledged Replication Write RPC Log Position Versus Time Since Start of Recovery"

set format y "%9.0f";
set format x "%9.0f";

set xlabel "ms"
set ylabel "MB"

set style line 1 lt 1 lw 1 pt 1 linecolor rgb "#E41A1C"
set style line 2 lt 2 lw 1 pt 2 linecolor rgb "#377EB8"
set style line 3 lt 3 lw 1 pt 3 linecolor rgb "#4DAF4A"
set style line 4 lt 4 lw 1 pt 4 linecolor rgb "#984EA3"
set style line 5 lt 5 lw 1 pt 5 linecolor rgb "#FF7F00"
set style line 6 lt 6 lw 1 pt 6 linecolor rgb "#FFFF33"
set style line 7 lt 7 lw 1 pt 7 linecolor rgb "#A65628"
set style line 8 lt 8 lw 1 pt 8 linecolor rgb "#F781BF"

plot "recovery-rpc-timeline.data" using ($1/1000):($2/1024/1024) with points ls 1 title "Replicated Log Position", \
     0 notitle
