#!/usr/bin/expect -f
spawn scp -P 12265 ParallelDijkstra.java PDNodeWritable.java PDPreProcess.java run_pre.sh hadoop@137.189.89.214:~/lzr_asgn2/shortestPath/
expect "hadoop@137.189.89.214's password: "
send "fjclzr\r"
interact
