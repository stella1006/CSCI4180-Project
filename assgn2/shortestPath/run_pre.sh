# #! /bin/bash
# if [ "$#" -eq 1 ]; then
#     rm ./pre_output/*
#     hadoop fs -rm /lzr/input/*
#     hadoop fs -put test.txt /lzr/input/
#     hadoop fs -rm -r /lzr/output
#     hadoop com.sun.tools.javac.Main PDPreProcess.java
#     jar cf pd.jar ParallelDijkstra*.class PD*.class
#     hadoop jar pd.jar PDPreProcess /lzr/input /lzr/output
#     hadoop fs -get /lzr/output/* pre_output/
# else
# rm ./pre_output/*
# hadoop fs -rm -r /lzr/output
# hadoop com.sun.tools.javac.Main PDPreProcess.java
# jar cf pdp.jar PDPreProcess*.class
# hadoop jar pdp.jar PDPreProcess /lzr/input /lzr/output
# hadoop fs -get /lzr/output/* pre_output/
# fi
rm ./pre_output/*
#hadoop fs -rm /lzr/input/*
#hadoop fs -put test.txt /lzr/input/
hadoop fs -rm -r /lzr/output
hadoop fs -rm -r /temp
hadoop com.sun.tools.javac.Main PDNodeWritable.java PDPreProcess.java ParallelDijkstra.java
jar cf pd.jar PD*.class ParallelDijkstra*.class
hadoop jar pd.jar ParallelDijkstra /lzr/input /lzr/output
# hadoop jar pd.jar ParallelDijkstra /lzr/input /lzr/output [src] [iterations]
hadoop fs -get /lzr/output/* pre_output/
