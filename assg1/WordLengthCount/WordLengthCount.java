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

public class WordLengthCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{

            private final static IntWritable one = new IntWritable(1);

            Map< IntWritable,IntWritable> hmap = new HashMap< IntWritable,IntWritable>();

            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                    IntWritable wordLength = new IntWritable(itr.nextToken().length());
                    if (!hmap.containsKey(wordLength)) {
                        hmap.put(wordLength, one);
                    }
                    else {
                        IntWritable temp = new IntWritable(hmap.get(wordLength).get()+1);
                        hmap.put(wordLength, temp);
                    }
                    //context.write(wordLength , one);
                }
            }

            public void cleanup(Context context) throws IOException, InterruptedException {
                if (!hmap.isEmpty()) {
                    for (Map.Entry<IntWritable, IntWritable> entry : hmap.entrySet()) {
                        context.write(entry.getKey() , entry.getValue());
                    }
                }
            }

    }

    public static class IntSumReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(IntWritable keyLength, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(keyLength, result);
            }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word length count");
        job.setJarByClass(WordLengthCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
