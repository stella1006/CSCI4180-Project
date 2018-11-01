#! /bin/bash
hadoop fs -rm -r /asgn1/output
jar cf bic.jar BigramInitialCount*.class
hadoop jar bic.jar BigramInitialCount /asgn1/input /asgn1/output
rm ./output/*
hadoop fs -get /asgn1/output/* output/
