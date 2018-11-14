
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PDPreProcess extends Mapper<Object, Text, PDNodeWritable, NullWritable> {
    //public static class ParsingMapper


        // public void setup(Context context) throws IOException, InterruptedException {
        //
        // }
        Map<Integer, PDNodeWritable> hmap = new HashMap<Integer,PDNodeWritable>();
        Map<IntWritable, Integer> flag = new HashMap<IntWritable, Integer>();

        @Override
        public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
            // String[] list=value.toString().split(" ");
            // Integer k = Integer.parseInt(list[0]);
            // IntWritable kk = new IntWritable(k);
            // IntWritable des = new IntWritable(Integer.parseInt(list[1]));
            // IntWritable weights = new IntWritable(Integer.parseInt(list[2]));
            StringTokenizer itr = new StringTokenizer(value.toString());
            Integer k = Integer.parseInt(itr.nextToken());
            IntWritable kk = new IntWritable(k);
            IntWritable des = new IntWritable(Integer.parseInt(itr.nextToken()));
            IntWritable weights = new IntWritable(Integer.parseInt(itr.nextToken()));

            //String outstring = "(" + Integer.toString(des) + " " + Integer.toString(weights) + ")";
            flag.put(kk, 1);
            flag.put(des, 1);

            if (!hmap.containsKey(k)) {
                PDNodeWritable node = new PDNodeWritable(kk);
                ArrayList<IntWritable> item = new ArrayList<IntWritable>();
                item.add(des);
                item.add(weights);
                node.addAdjList(item);
                //hmap.put(k, new Text(outstring));
                hmap.put(k, node);
            }
            else {
                PDNodeWritable temp_node = hmap.get(k);
                ArrayList<IntWritable> item = new ArrayList<IntWritable>();
                item.add(des);
                item.add(weights);
                temp_node.addAdjList(item);
                // // String temp_node = hmap.get(k).toString();
                // // temp_node += "," + outstring;
                // //hmap.replace(k, new Text(temp_node));
                hmap.replace(k, temp_node);
            }
            //Text te = new Text(outstring);
            //context.write(k , te);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            if (!flag.isEmpty()) {
                for (Map.Entry<IntWritable, Integer> entry : flag.entrySet()) {
                    if (!hmap.containsKey(entry.getKey().get())) {
                        PDNodeWritable node = new PDNodeWritable(entry.getKey());
                        hmap.put(entry.getKey().get(), node);
                    }
                }
            }
            if (!hmap.isEmpty()) {
                for (Map.Entry<Integer, PDNodeWritable> entry : hmap.entrySet()) {
                    context.write(entry.getValue() , NullWritable.get());
                    //context.write(new IntWritable(entry.getKey()) , NullWritable.get());
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
