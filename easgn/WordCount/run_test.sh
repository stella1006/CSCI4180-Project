#! /bin/bash
if [ "$#" -eq 1 ]; then
    echo "Make"
    rm WC.jar
    rm -r WCount
    mkdir WCount
    javac -classpath ~/spark-1.6.3-bin-hadoop2.6/lib/spark-assembly-1.6.3-hadoop2.6.0.jar WordCount.java -d WCount
    jar -cvf WC.jar -C WCount .
    echo "Make Done"
else
rm ./output/*
hadoop fs -rm -r /easgn1/output
spark-submit --class WordCount --master yarn --deploy-mode cluster --executor-memory 800m WC.jar /easgn1/input /easgn1/output
hadoop fs -get /easgn1/output/* ./output/
fi
