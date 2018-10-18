echo "Make"
rm BIRF.jar
rm -r BigramIRF
mkdir BigramIRF
javac -classpath ~/spark-1.6.3-bin-hadoop2.6/lib/spark-assembly-1.6.3-hadoop2.6.0.jar BigramInitialRF.java -d BigramIRF
jar -cvf BIRF.jar -C BigramIRF .
echo "Make Done"
