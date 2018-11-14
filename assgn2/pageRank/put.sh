#!/usr/bin/expect -f
spawn scp -P 12265 PageRank.java PRNodeWritable.java PRPreProcess.java PRAdjust.java run.sh hadoop@137.189.89.214:~/lzr_asgn2/pageRank/
expect "hadoop@137.189.89.214's password: "
send "fjclzr\r"
interact
