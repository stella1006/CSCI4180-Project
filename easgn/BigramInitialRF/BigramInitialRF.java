import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.lang.Iterable;
import java.util.HashMap;
import java.util.ArrayList;

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

public class BigramInitialRF {
    public static void main(String[] args) throws Exception {
        if(args.length < 2){
			System.err.println("Require Input Arguments: [Input Path] [Output Path]");
			System.exit(1);
		}

		String inputFile = args[0];
    	String outputFile = args[1];

        // Create a Java Spark Context.
    	SparkConf conf = new SparkConf().setAppName("BigramInitialRF");
        conf.set("spark.testing.memory", "2147480000");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load our input data.
    	JavaRDD<String> input = sc.textFile(inputFile);

        String deli = new String("[^a-zA-Z]+");


        // Split up into words.
        // JavaPairRDD<String,Tuple2<Integer, Integer>> counts = input.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<Integer, Integer>>() {
        //     String first_term = new String("");
        //     Map< String, Integer> hmap = new HashMap< String,Integer>();
        //     Map< String, Integer> totalMap = new HashMap< String,Integer>();
        //
        //     @Override
        //     public Iterable<Tuple2<String, Tuple2<Integer, Integer>>> call(String s) throws Exception {
        //         String[] temp=s.split(deli);
        //         ArrayList<Tuple2<String,Tuple2<Integer, Integer>>> list = new ArrayList<Tuple2<String,Tuple2<Integer, Integer>>>();
        //         String second_term = new String("");
        //         for (String item : temp) {
        //             if (item.length() <= 0) continue;
        //             if (first_term.length() > 0) {
        //                 second_term = item;
        //                 String Cap = Character.toString(first_term.charAt(0));
        //                 String st = Character.toString(first_term.charAt(0)) + " " + Character.toString(second_term.charAt(0));
        //
        //                 if (!totalMap.containsKey(Cap)) {
        //                     totalMap.put(Cap, 1);
        //                 }
        //                 else {
        //                     totalMap.put(Cap, totalMap.get(Cap)+1);
        //                 }
        //
        //                 if (!hmap.containsKey(st)) {
        //                     hmap.put(st, 1);
        //                 }
        //                 else {
        //                     hmap.put(st, hmap.get(st)+1);
        //                 }
        //
        //                 first_term = item;
        //             } else {
        //                 first_term = item;
        //             }
        //         }
        //
        //         for (Map.Entry<String, Integer> entry : hmap.entrySet()) {
        //             String st =  (entry.getKey());
        //             String cap = Character.toString((st.charAt(0)));
        //             list.add(new Tuple2<String,Tuple2<Integer, Integer>>(st, new Tuple2<Integer,Integer>(entry.getValue(),totalMap.get(cap))));
        //         }
        //         return list;
        //     }
        // });


        JavaPairRDD<String,Tuple2<String, Integer>> counts = input.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<String, Integer>>() {
            String first_term = new String("");

            @Override
            public Iterable<Tuple2<String, Tuple2<String, Integer>>> call(String s) throws Exception {
                String[] temp=s.split(deli);
                ArrayList<Tuple2<String, Tuple2<String, Integer>>> list = new ArrayList<Tuple2<String, Tuple2<String, Integer>>>();
                String second_term = new String("");
                for (String item : temp) {
                    if (item.length() <= 0) continue;
                    if (first_term.length() > 0) {
                        second_term = item;
                        list.add(new Tuple2<String, Tuple2<String, Integer>>(Character.toString(first_term.charAt(0)), new Tuple2<String,Integer>((
                                                            Character.toString(second_term.charAt(0))),1)));
                        first_term = item;
                    } else {
                        first_term = item;
                    }
                }
                return list;
            }
        });

        counts.combineByKey(
            (v) => (1, v),
            (acc: (Int, String), v) => (acc._1 + 1, acc._2),
            (acc1: (Int, String), acc2: (Int, String)) => (acc1._1 + acc2._1, acc2._2
        );//.map { case (name, (num, socre)) => (name, socre / num) }.collect

        // JavaPairRDD<String, Double> freq = counts.map({case (key,(X1,X2)) => (key)-> (X1/X2)});
        // Save the result back to the specific output file.
        counts.saveAsTextFile(outputFile);

        sc.stop();
    }
}
