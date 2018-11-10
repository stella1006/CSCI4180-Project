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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

public class ParallelDijkstra {
    public static class DijkstraMapper
            extends Mapper<Object, Text, IntWritable, PDNodeWritable>{
                public void setup(Context context) throws IOException, InterruptedException {

                }

                public void map(Object key, PDNodeWritable value, Context context
                        ) throws IOException, InterruptedException {
                            context.write(new IntWritable(0), value);
                        }
                public void cleanup(Context context) throws IOException, InterruptedException {

                }
            }

    public static class DijkstraReducer
            extends Reducer<IntWritable,PDNodeWritable,NullWritable,PDNodeWritable> {

            public void reduce(IntWritable key, Iterable<PDNodeWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                        for (PDNodeWritable val : values) {
                            context.write(NullWritable.get(), val);
                        }
                    }
    }

    public static void main(String[] args) throws Exception {
        //Configuration conf1 = new Configuration();
        //Configuration conf2 = new Configuration();

        JobControl jobControl = new JobControl("jobChain");

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);
        Job job2 = Job.getInstance(conf);

        job1.setJarByClass(ParallelDijkstra.class);
        job1.setJobName("PD PreProcess");

        job1.setMapperClass(PDPreProcess.class);
        //job1.setCombinerClass(ComReducer.class);
        job1.setNumReduceTasks(0);
        // job1.setReducerClass(ComReducer.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(PDNodeWritable.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("/temp"));


        job2.setJarByClass(ParallelDijkstra.class);
        job2.setJobName("ParallelDijkstra");

        job2.setMapperClass(DijkstraMapper.class);
        job2.setCombinerClass(DijkstraReducer.class);
        job2.setReducerClass(DijkstraReducer.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(PDNodeWritable.class);
        FileInputFormat.addInputPath(job2, new Path("/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        ControlledJob controlledJob1 = new ControlledJob(conf);
        ControlledJob controlledJob2 = new ControlledJob(conf);

        controlledJob1.setJob(job1);
        controlledJob2.setJob(job2);
        // make job2 dependent on job1
        controlledJob2.addDependingJob(controlledJob1);

        // add the job to the job control
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        Thread jcThread = new Thread(jobControl);
        jcThread.start();

        //while(true){
        while (!jobControl.allFinished()){
            //System.out.println(jobControl.getSuccessfulJobList());
            //System.exit(0);
        }
        System.out.println(jobControl.getSuccessfulJobList());
        jobControl.stop();
        //}
    }
}
