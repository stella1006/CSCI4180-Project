#! /bin/bash
if [ "$#" -eq 1 ]; then
    rm ./output/*
    hadoop fs -rm /lzr_data/input/*
    hadoop fs -put test.txt /lzr_data/input/
    hadoop fs -rm -r /lzr_data/output
    spark-submit --class BigramInitialCount BIC.jar /lzr_data/input /lzr_data/output
    hadoop fs -get /lzr_data/output/* ./output/
else
rm ./output/*
hadoop fs -rm -r /easgn1/output
spark-submit --class BigramInitialCount BIC.jar /easgn1/input /easgn1/output
hadoop fs -get /easgn1/output/* ./output/
fi
