#! /bin/bash
if [ "$#" -eq 1 ]; then
    hadoop fs -rm /lzr/input/*
    hadoop fs -put ./twitter/twitter_dist_1.txt /lzr/input/
    #hadoop fs -put ./twitter/twitter_dist_2.txt /lzr/input/
    #hadoop fs -put ./twitter/twitter_dist_3.txt /lzr/input/
else
    hadoop fs -rm /lzr/input/*
    hadoop fs -put test.txt /lzr/input/
fi

rm -rf ./output/*
rm -rf ./temp_out/*
hadoop fs -rm -r  /lzr/output
hadoop fs -rm -r  /lzr/temp/*
hadoop com.sun.tools.javac.Main PRNodeWritable.java PRPreProcess.java PRAdjust.java PageRank.java
jar cf pr.jar PR*.class PageRank*.class
time hadoop jar pr.jar PageRank  0.2 0 1 /lzr/input /lzr/output
hadoop fs -get /lzr/output/* output/
hadoop fs -get /lzr/temp/* temp_out/
