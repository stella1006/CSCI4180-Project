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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;

public class BigramInitialCount {
    public static void main(String[] args) throws Exception {
        if(args.length < 2){
			System.err.println("Require Input Arguments: [Input Path] [Output Path]");
			System.exit(1);
		}

		String inputFile = args[0];
    	String outputFile = args[1];

        // Create a Java Spark Context.
    	SparkConf conf = new SparkConf().setAppName("BigramInitialCount");
        conf.set("spark.testing.memory", "2147480000");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load our input data.
    	JavaRDD<String> input = sc.textFile(inputFile);

        String deli = new String("[^a-zA-Z]+");

		// Split up into words.
        JavaPairRDD<String,Integer> counts = input.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            String first_term = new String("");

            @Override
            public Iterable<Tuple2<String, Integer>> call(String s) throws Exception {
                String[] temp=s.split(deli);
                ArrayList<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String,Integer>>();
                String second_term = new String("");
                for (String item : temp) {
                    if (item.length() <= 0) continue;
                    if (first_term.length() > 0) {
                        second_term = item;
                        list.add(new Tuple2<String,Integer>((Character.toString(first_term.charAt(0))+
                                                            " "+Character.toString(second_term.charAt(0))),1));
                        first_term = item;
                    } else {
                        first_term = item;
                    }
                }
                return list;
            }
        }).reduceByKey((x, y) -> x+y);

        // Save the result back to the specific output file.
        counts.saveAsTextFile(outputFile);

        sc.stop();
    }
}
