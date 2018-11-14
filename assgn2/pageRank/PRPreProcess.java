
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PRPreProcess extends Mapper<Object, Text, PRNodeWritable, NullWritable> {
        Map<Integer, PRNodeWritable> hmap = new HashMap<Integer,PRNodeWritable>();
        Map<IntWritable, Integer> flag = new HashMap<IntWritable, Integer>();
        Integer totalCount = 0;

        @Override
        public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            Integer k = Integer.parseInt(itr.nextToken());
            IntWritable kk = new IntWritable(k);
            IntWritable des = new IntWritable(Integer.parseInt(itr.nextToken()));

            flag.put(kk, 1);
            flag.put(des, 1);

            if (!hmap.containsKey(k)) {
                totalCount++;
                PRNodeWritable node = new PRNodeWritable(kk);
                node.addAdjList(des);
                hmap.put(k, node);
            }
            else {
                PRNodeWritable temp_node = hmap.get(k);
                temp_node.addAdjList(des);
                hmap.replace(k, temp_node);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            if (!flag.isEmpty()) {
                for (Map.Entry<IntWritable, Integer> entry : flag.entrySet()) {
                    if (!hmap.containsKey(entry.getKey().get())) {
                        totalCount++;
                        PRNodeWritable node = new PRNodeWritable(entry.getKey());
                        hmap.put(entry.getKey().get(), node);
                    }
                }
            }
            if (!hmap.isEmpty()) {
                DoubleWritable pageInitialValue = new DoubleWritable(1.0/totalCount);
                for (Map.Entry<Integer, PRNodeWritable> entry : hmap.entrySet()) {
                    entry.getValue().setPageRank(pageInitialValue);
                    context.write(entry.getValue() , NullWritable.get());
                    //context.write(new IntWritable(entry.getKey()) , NullWritable.get());
                }
            }
        }

}
