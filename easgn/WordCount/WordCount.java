import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


public class WordCount {
    public static void main(String[] args) throws Exception {

		if(args.length < 2){
			System.err.println("Require Input Arguments: [Input Path] [Output Path]");
			System.exit(1);
		}
			
		String inputFile = args[0];
    	String outputFile = args[1];
    
		// Create a Java Spark Context.
    	SparkConf conf = new SparkConf().setAppName("WordCount");
	conf.set("spark.testing.memory", "2147480000");
		JavaSparkContext sc = new JavaSparkContext(conf);
    	
		// Load our input data.
    	JavaRDD<String> input = sc.textFile(inputFile);
    
		// Split up into words.
        JavaRDD<String> words = input.flatMap(
            new FlatMapFunction<String, String>() {
                public Iterable<String> call(String x) {
                    return Arrays.asList(x.split(" "));
        }});
    	
		// the mapToPair function get the  words rdd and produce a <word,num> pairRDD
		// the reduceByKey function counts the <words, num> pariRDD and produce a new pairRDD
        JavaPairRDD<String, Integer> counts = words.mapToPair(
            new PairFunction<String, String, Integer>(){
                public Tuple2<String, Integer> call(String x){
                    return new Tuple2(x, 1);
        }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
                public Integer call(Integer x, Integer y){ return x + y;}});

        // Save the result back to the specific output file.
        counts.saveAsTextFile(outputFile);
		
		//Below are wordcount code that use Lambda Expression
		/*		
		JavaPairRDD<String, Integer> counts = input.flatMap(x -> Arrays.asList(x.split(" ")))
							    .mapToPair(x -> new Tuple2<String, Integer>(x,1))
							    .reduceByKey((x, y) -> x+y);
	    counts.saveAsTextFile(outputFile);
		*/
		sc.stop();
	}
}
