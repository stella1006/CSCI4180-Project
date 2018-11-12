import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.LongWritable;


class PDNodeecordReader extends RecordReader<IntWritable,PDNodeWritable>  {
    LineRecordReader lineRecordReader;
    IntWritable key;
    PDNodeWritable value;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader = new LineRecordReader();
        lineRecordReader.initialize(inputSplit, context);
    }

    // It’s the method that will be used to transform the line into key value
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!lineRecordReader.nextKeyValue()) {
            return false;
        }
        String line = new String(lineRecordReader.getCurrentValue().toString());
        //String[] keyValue = line.split("\t");
        //String[] keyFields = keyValue[0].split(" ");
        //valueFields = keyValue[1].split(" ");
        //IntWritable temp = new IntWritable(line);
        value = new PDNodeWritable();
        //key.setText(line);
        value.setFromText(line);
        key = value.getNodeId();
        //IntWritable nodeId = temp.getNodeId();
        //key = new PDNodeWritable(nodeId);
        //value = new PDNodeWritable(IntWritable);
        return true;
    }

    @Override
        public IntWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public PDNodeWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {
            lineRecordReader.close();
        }
}

class PDNodeInputFormat extends FileInputFormat<IntWritable,PDNodeWritable> {
      @Override
      public RecordReader<IntWritable,PDNodeWritable> createRecordReader(InputSplit inputSplit,TaskAttemptContext context)
          throws IOException, InterruptedException {

          PDNodeecordReader pdRecordReader = new PDNodeecordReader();
          pdRecordReader.initialize(inputSplit, context);
          return pdRecordReader;
      }
  }


public class ParallelDijkstra {
    public static enum ReachCounter { COUNT };

    public static class DijkstraMapper
            extends Mapper<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable>{

                public void setup(Context context) throws IOException, InterruptedException {
                }

                public void map(IntWritable key, PDNodeWritable value, Context context
                        ) throws IOException, InterruptedException {
                            //context.write(value.getNodeId(), value);
                            Configuration conf = context.getConfiguration();
                            int src = Integer.parseInt(conf.get("src"));

                            if (key.get() == src) {
                                value.setDistance(new IntWritable(0));
                            }
                            IntWritable d = value.getDistance();
                            context.write(key, value);

                            //for (ArrayList<IntWritable> node : value.getAdjList()) {
                            for (int i = 0; i < value.getAdjList().size(); i++) {
                                ArrayList<IntWritable> node = value.getAdjList().get(i);
                                IntWritable tmp_id = new IntWritable(0);
                                IntWritable tmp_dis = new IntWritable(0);
                                if (d.get() == Integer.MAX_VALUE) {
                                    tmp_dis.set(Integer.MAX_VALUE);
                                } else {
                                    tmp_dis.set(d.get()+node.get(1).get());
                                }
                                PDNodeWritable pass = new PDNodeWritable(tmp_id, tmp_dis, key);
                                context.write(node.get(0), pass);
                            }

                        }
                public void cleanup(Context context) throws IOException, InterruptedException {

                }
            }

    public static class DijkstraReducer
            extends Reducer<IntWritable,PDNodeWritable,NullWritable,PDNodeWritable> {

            public void reduce(IntWritable key, Iterable<PDNodeWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                    // for (PDNodeWritable val : values) {
                    //     context.write(key, val);
                    // }

                    IntWritable d_min = new IntWritable((Integer.MAX_VALUE));
                    IntWritable rec_prev = new IntWritable(0);
                    PDNodeWritable M = new PDNodeWritable();
                    for (PDNodeWritable node : values) {
                        if (node.getNodeId().get() != 0) {
                            M.setNode(node);
                            //context.write(key, M);
                            //d_min.set(M.getDistance().get());
                        }
                        else if (node.getDistance().get() < d_min.get()) {
                            d_min.set(node.getDistance().get());
                            rec_prev.set(node.getPrevId().get());
                        }
                    }
                    if (d_min.get() < M.getDistance().get()) {
                        context.getCounter(ReachCounter.COUNT).increment(1);
                        M.setDistance(d_min);
                        M.setPrevId(rec_prev);
                    }
                    context.write(NullWritable.get(), M);
                }
        }

    public static class FinalReducer
            extends Reducer<IntWritable,PDNodeWritable,NullWritable,Text> {

            public void reduce(IntWritable key, Iterable<PDNodeWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                    // for (PDNodeWritable val : values) {
                    //     context.write(key, val);
                    // }
                    IntWritable d_min = new IntWritable((Integer.MAX_VALUE));
                    IntWritable rec_prev = new IntWritable(0);
                    PDNodeWritable M = new PDNodeWritable();
                    for (PDNodeWritable node : values) {
                        if (node.getNodeId().get() != 0) {
                            M.setNode(node);
                        }
                        else if (node.getDistance().get() < d_min.get()) {
                            d_min.set(node.getDistance().get());
                            rec_prev.set(node.getPrevId().get());
                        }
                    }
                    if (d_min.get() < M.getDistance().get()) {
                        context.getCounter(ReachCounter.COUNT).increment(1);
                        M.setDistance(d_min);
                        M.setPrevId(rec_prev);
                    }
                    Text result = new Text("");
                    if (M.getDistance().get() != Integer.MAX_VALUE) {
                        String prev = "";
                        if (M.getPrevId().get() == 0) {
                            prev = "nil";
                        } else {
                            prev = M.getPrevId().toString();
                        }
                        result.set(M.getNodeId().toString()+ " " + M.getDistance().toString() + " " + prev);
                        context.write(NullWritable.get(), new Text(result));
                    }
                    //context.write(NullWritable.get(), M);
                }
    }

    public static void main(String[] args) throws Exception {
        //Configuration conf1 = new Configuration();
        //Configuration conf2 = new Configuration();
        int iterations = Integer.parseInt(args[3]);


        JobControl jobControl = new JobControl("jobChain");

        Configuration conf = new Configuration();
        conf.set("src", args[2]);
        conf.set("iterations", args[3]);

        Job job1 = Job.getInstance(conf);
        Job job2 = Job.getInstance(conf);

        job1.setJarByClass(ParallelDijkstra.class);
        job1.setJobName("PD PreProcess");

        job1.setMapperClass(PDPreProcess.class);
        //job1.setCombinerClass(ComReducer.class);
        job1.setNumReduceTasks(0);
        // job1.setReducerClass(ComReducer.class);
        job1.setOutputKeyClass(PDNodeWritable.class);
        job1.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("/lzr/temp/out_0"));


        job2.setJarByClass(ParallelDijkstra.class);
        job2.setJobName("ParallelDijkstra");

        job2.setMapperClass(DijkstraMapper.class);
        //job2.setCombinerClass(DijkstraReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(PDNodeWritable.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setInputFormatClass(PDNodeInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path("/lzr/temp/out_0"));

        if (iterations == 1) {
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            job2.setReducerClass(FinalReducer.class);
            job2.setOutputValueClass(Text.class);
        } else {
            FileOutputFormat.setOutputPath(job2, new Path("/lzr/temp/out_1"));
            job2.setReducerClass(DijkstraReducer.class);
            job2.setOutputValueClass(PDNodeWritable.class);
        }

        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        ControlledJob controlledJob1 = new ControlledJob(conf);
        ControlledJob controlledJob2 = new ControlledJob(conf);

        //job1.waitForCompletion(true);
        controlledJob1.setJob(job1);
        controlledJob2.setJob(job2);
        // make job2 dependent on job1
        controlledJob2.addDependingJob(controlledJob1);

        // add the job to the job control
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        Thread jcThread = new Thread(jobControl);
        jcThread.start();

        long temp_counts;
        //while(true){
        while(true){
            if(jobControl.allFinished()){
                temp_counts = job2.getCounters().findCounter(ParallelDijkstra.ReachCounter.COUNT ).getValue();
                System.out.println(temp_counts);
                System.out.println(jobControl.getSuccessfulJobList());
                jobControl.stop();
                if(jobControl.getFailedJobList().size() > 0){
                    System.out.println(jobControl.getFailedJobList());
                }
                break;
            }
        }
        // if (iterations != 0) {
        if (iterations == 0) {
            iterations = Integer.MAX_VALUE;
        }
        int flag = 0;
        for (int i = 1; i < iterations; i++) {
            if (flag == 2) break;
            String last_out = "/lzr/temp/out_" + Integer.toString(i);
            String next_out = "";

            Job job_new = new Job(conf, "ParallelDijkstra_" + i);
            job_new.setJarByClass(ParallelDijkstra.class);
            job_new.setMapperClass(DijkstraMapper.class);
            //job_new.setCombinerClass(DijkstraReducer.class);
            if (i == iterations-1 || flag == 1) {
                next_out = args[1];
                job_new.setReducerClass(FinalReducer.class);
                job_new.setOutputValueClass(Text.class);
            } else {
                next_out = "/lzr/temp/out_" + Integer.toString(i+1);
                job_new.setReducerClass(DijkstraReducer.class);
                job_new.setOutputValueClass(PDNodeWritable.class);
            }
            job_new.setMapOutputKeyClass(IntWritable.class);
            job_new.setMapOutputValueClass(PDNodeWritable.class);
            job_new.setOutputKeyClass(NullWritable.class);
            job_new.setInputFormatClass(PDNodeInputFormat.class);

            FileInputFormat.addInputPath(job_new, new Path(last_out));
            FileOutputFormat.setOutputPath(job_new, new Path(next_out));
            job_new.waitForCompletion(true);

            temp_counts = job_new.getCounters().findCounter(ParallelDijkstra.ReachCounter.COUNT ).getValue();
            System.out.println("Finish_" + i + "_" + temp_counts);
            if (flag == 1) flag = 2;
            if (temp_counts == 0 && flag == 0) {
                flag = 1;
            }
        }
    }
}
