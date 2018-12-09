#!/usr/bin/expect -f
spawn scp -P 12265 download.sh delete.sh upload.sh remove.sh make.sh MyDedup.java run.sh hadoop@137.189.89.214:~/lzr_asgn3
expect "hadoop@137.189.89.214's password: "
send "fjclzr\r"
interact
