echo "Make"
rm BIC.jar
rm -r BigramIC
mkdir BigramIC
javac -classpath ~/spark-1.6.3-bin-hadoop2.6/lib/spark-assembly-1.6.3-hadoop2.6.0.jar BigramInitialCount.java -d BigramIC
jar -cvf BIC.jar -C BigramIC .
echo "Make Done"
