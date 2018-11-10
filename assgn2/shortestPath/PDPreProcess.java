
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PDPreProcess extends Mapper<Object, Text, NullWritable, PDNodeWritable> {
    //public static class ParsingMapper


        // public void setup(Context context) throws IOException, InterruptedException {
        //
        // }
        Map< Integer, PDNodeWritable> hmap = new HashMap< Integer,PDNodeWritable>();
        @Override
        public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
            String[] list=value.toString().split(" ");
            Integer k = Integer.parseInt(list[0]);
            Integer des = Integer.parseInt(list[1]);
            Integer weights = Integer.parseInt(list[2]);
            //String outstring = "(" + Integer.toString(des) + " " + Integer.toString(weights) + ")";

            if (!hmap.containsKey(k)) {
                PDNodeWritable node = new PDNodeWritable(k);
                ArrayList<Integer> item = new ArrayList<Integer>();
                item.add(des);
                item.add(weights);
                node.addAdjList(item);
                hmap.put(k, node);
            }
            else {
                PDNodeWritable temp_node = hmap.get(k);
                ArrayList<Integer> item = new ArrayList<Integer>();
                item.add(des);
                item.add(weights);
                temp_node.addAdjList(item);
                hmap.replace(k, temp_node);
            }
            //Text te = new Text(outstring);
            //context.write(k , te);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            if (!hmap.isEmpty()) {
                for (Map.Entry<Integer, PDNodeWritable> entry : hmap.entrySet()) {
                    context.write(NullWritable.get() , entry.getValue());
                }
            }
        }




    // public static class ComReducer
    //         extends Reducer<IntWritable,Text,IntWritable,Text> {
    //
    //         public void reduce(IntWritable key, Iterable<Text> values,
    //                 Context context
    //                 ) throws IOException, InterruptedException {
    //                     String result = "";
    //                     for (Text val : values) {
    //                         if (result.length()!=0) {
    //                             result+=",";
    //                         }
    //                         result+=val.toString();
    //                     }
    //                     context.write(key , new Text(result));
    //                 }
    // }
    //
    // public static void main(String[] args) throws Exception {
    //     Configuration conf = new Configuration();
    //     Job job = Job.getInstance(conf, "PD PreProcess");
    //     job.setJarByClass(PDPreProcess.class);
    //     job.setMapperClass(ParsingMapper.class);
    //     job.setCombinerClass(ComReducer.class);
    //     job.setReducerClass(ComReducer.class);
    //     job.setOutputKeyClass(IntWritable.class);
    //     job.setOutputValueClass(Text.class);
    //     FileInputFormat.addInputPath(job, new Path(args[0]));
    //     FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //     System.exit(job.waitForCompletion(true) ? 0 : 1);
    // }
}
