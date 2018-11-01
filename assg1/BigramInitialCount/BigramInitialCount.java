import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigramInitialCount {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

            private final static IntWritable one = new IntWritable(1);
            private final String deli = new String("[^a-zA-Z]+");

            Map< Text, IntWritable> hmap = new HashMap< Text,IntWritable>();
            String first_term = new String("");

            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                String[] spl = value.toString().split(deli);
                String second_term = new String("");
                for (String item : spl) {
                    if (item.length() <= 0) continue;
                    if (first_term.length() > 0) {
                        second_term = item;
                        String st = Character.toString(first_term.charAt(0)) + " " + Character.toString(second_term.charAt(0));
                        Text text = new Text(st);

                        if (!hmap.containsKey(text)) {
                            hmap.put(text, one);
                        }
                        else {
                            IntWritable temp_num = new IntWritable(hmap.get(text).get()+1);
                            hmap.put(text, temp_num);
                        }
                    }
                    first_term = item;
                }
            }

            public void cleanup(Context context) throws IOException, InterruptedException {
                if (!hmap.isEmpty()) {
                    for (Map.Entry<Text, IntWritable> entry : hmap.entrySet()) {
                        context.write(entry.getKey() , entry.getValue());
                    }
                }
            }

    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(Text keyPair, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(keyPair, result);
            }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bigram initial count");
        job.setJarByClass(BigramInitialCount.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
