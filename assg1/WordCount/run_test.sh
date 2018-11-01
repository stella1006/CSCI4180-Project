#! /bin/bash
hadoop fs -rm -r /asgn1/output
hadoop jar wc.jar WordCount /asgn1/input /asgn1/output
rm ./output/*
hadoop fs -get /asgn1/output/* output/
