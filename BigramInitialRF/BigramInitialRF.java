import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigramInitialRF {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

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
                Map< Text, IntWritable> RF = new HashMap< Text,IntWritable>();
                IntWritable one = new IntWritable(1);

                for (Map.Entry<Text, IntWritable> entry : hmap.entrySet()) {
                    String str = entry.getKey().toString();
                    IntWritable num = entry.getValue();
                    Text text = new Text(Character.toString(str.charAt(0)));

                    if (!RF.containsKey(text)) {
                        RF.put(text, num);
                    }
                    else {
                        IntWritable temp_num = new IntWritable(RF.get(text).get() + num.get());
                        RF.put(text, temp_num);
                    }
                }

                if (!hmap.isEmpty()) {
                    for (Map.Entry<Text, IntWritable> entry : hmap.entrySet()) {
                        String str = entry.getKey().toString();
                        Text text = new Text(Character.toString(str.charAt(0)));
                        String num = entry.getValue().toString();
                        String total = RF.get(text).toString();
                        Text result = new Text(num + " " + total);
                        // if (fre >= theta) {
                        context.write(entry.getKey() , result);
                        // }
                    }
                }
            }

    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,DoubleWritable> {
            private DoubleWritable result = new DoubleWritable();

            public void reduce(Text keyPair, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                double theta = Double.parseDouble(conf.get("theta"));

                int self_num = 0;
                int sum = 0;

                for (Text val : values) {
                    String temp = val.toString();
                    String[] spl = temp.toString().split("\\s");
                    int num = Integer.parseInt(spl[0]);
                    int total = Integer.parseInt(spl[1]);
                    self_num += num;
                    sum += total;
                }
                result.set(self_num*1.0/sum);
                if (result.get() >= theta) {
                    context.write(keyPair, result);
                }

            }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("theta", args[2]);

        Job job = Job.getInstance(conf, "Bigram initial RF");
        job.setJarByClass(BigramInitialRF.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
