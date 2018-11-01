#! /bin/bash
hadoop fs -rm -r /asgn1/output
jar cf birf.jar BigramInitialRF*.class
hadoop jar birf.jar BigramInitialRF /asgn1/input /asgn1/output 0.1
rm ./output/*
hadoop fs -get /asgn1/output/* output/
