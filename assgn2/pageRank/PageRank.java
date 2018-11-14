import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;
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


class PRNodeecordReader extends RecordReader<IntWritable,PRNodeWritable>  {
    LineRecordReader lineRecordReader;
    IntWritable key;
    PRNodeWritable value;

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
        value = new PRNodeWritable();
        //key.setText(line);
        value.setFromText(line);
        key = value.getNodeId();
        //IntWritable nodeId = temp.getNodeId();
        //key = new PRNodeWritable(nodeId);
        //value = new PRNodeWritable(IntWritable);
        return true;
    }

    @Override
        public IntWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public PRNodeWritable getCurrentValue() throws IOException, InterruptedException {
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

class PRNodeInputFormat extends FileInputFormat<IntWritable,PRNodeWritable> {
      @Override
      public RecordReader<IntWritable,PRNodeWritable> createRecordReader(InputSplit inputSplit,TaskAttemptContext context)
          throws IOException, InterruptedException {

          PRNodeecordReader prRecordReader = new PRNodeecordReader();
          prRecordReader.initialize(inputSplit, context);
          return prRecordReader;
      }
  }

    public class PageRank {
        public static enum ReachCounter { COUNT };
        public static class PageRankMapper
              extends Mapper<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable>{
                  public void map(IntWritable key, PRNodeWritable value, Context context
                          ) throws IOException, InterruptedException {
                              //context.write(value.getNodeId(), value);
                              if (value.getAdjList().size() > 0) {
                                  DoubleWritable p = new DoubleWritable(value.getPageRankValue().get() / value.getAdjList().size());
                                  context.write(value.getNodeId(), value);

                                  for (IntWritable m : value.getAdjList()) {
                                      context.write(m, new PRNodeWritable(new IntWritable(0), p));
                                  }
                              }
                          }
              }
        public static class PageRankReducer
              extends Reducer<IntWritable,PRNodeWritable,NullWritable,PRNodeWritable> {
                  public void reduce(IntWritable key, Iterable<PRNodeWritable> values,
                          Context context
                          ) throws IOException, InterruptedException {
                              // for (PRNodeWritable val : values) {
                              //     context.write(key, val);
                              // }
                              PRNodeWritable M = new PRNodeWritable();
                              Double s = 0.0;
                              for (PRNodeWritable item: values) {
                                  if (item.getNodeId().get() != 0) {
                                      M.setNode(item);
                                  }
                                  else {
                                      s += item.getPageRankValue().get();
                                  }
                              }
                              M.setPageRank(new DoubleWritable(s));
                              context.write(NullWritable.get(), M);
                          }
              }

        public static void main(String[] args) throws Exception {
            int iterations = Integer.parseInt(args[2]);

            JobControl jobControl = new JobControl("jobChain");

            Configuration conf = new Configuration();
            conf.set("alpha", args[0]);
            conf.set("threshold", args[1]);
            conf.set("iterations", args[2]);

            Job job1 = Job.getInstance(conf);
            Job job2 = Job.getInstance(conf);
            Job job3 = Job.getInstance(conf);

            job1.setJarByClass(PageRank.class);
            job1.setJobName("PR PreProcess");

            job1.setMapperClass(PRPreProcess.class);
            //job1.setCombinerClass(ComReducer.class);
            job1.setNumReduceTasks(0);
            // job1.setReducerClass(ComReducer.class);
            job1.setOutputKeyClass(PRNodeWritable.class);
            job1.setOutputValueClass(NullWritable.class);
            FileInputFormat.setInputPaths(job1, new Path(args[3]));
            FileOutputFormat.setOutputPath(job1, new Path("/lzr/temp/out_0"));
            //job1.waitForCompletion(true);


            job2.setJarByClass(PageRank.class);
            job2.setJobName("PageRank");

            job2.setMapperClass(PageRankMapper.class);
            //job2.setCombinerClass(DijkstraReducer.class);
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(PRNodeWritable.class);
            job2.setReducerClass(PageRankReducer.class);
            job2.setOutputKeyClass(NullWritable.class);
            job2.setOutputValueClass(PRNodeWritable.class);
            job2.setInputFormatClass(PRNodeInputFormat.class);
            FileInputFormat.addInputPath(job2, new Path("/lzr/temp/out_0"));
            FileOutputFormat.setOutputPath(job2, new Path("/lzr/temp/out_1"));
            //job2.waitForCompletion(true);

            job3.setJarByClass(PageRank.class);
            job3.setJobName("PageAdjust");

            job3.setMapperClass(PRAdjust.class);
            job3.setNumReduceTasks(0);
            //job2.setCombinerClass(DijkstraReducer.class);
            //job2.setMapOutputKeyClass(IntWritable.class);
            //job2.setMapOutputValueClass(PRNodeWritable.class);
            //job2.setReducerClass(PRAdjust$PRAdjustReducer.class);
            job3.setOutputKeyClass(NullWritable.class);
            job3.setOutputValueClass(PRNodeWritable.class);
            job3.setInputFormatClass(PRNodeInputFormat.class);
            FileInputFormat.addInputPath(job3, new Path("/lzr/temp/out_1"));
            FileOutputFormat.setOutputPath(job3, new Path(args[4]));



            ControlledJob controlledJob1 = new ControlledJob(conf);
            ControlledJob controlledJob2 = new ControlledJob(conf);
            ControlledJob controlledJob3 = new ControlledJob(conf);
            controlledJob1.setJob(job1);
            controlledJob2.setJob(job2);
            controlledJob3.setJob(job3);
            // make job2 dependent on job1
            controlledJob2.addDependingJob(controlledJob1);
            controlledJob3.addDependingJob(controlledJob2);

            // add the job to the job control
            jobControl.addJob(controlledJob1);
            jobControl.addJob(controlledJob2);
            jobControl.addJob(controlledJob3);
            Thread jcThread = new Thread(jobControl);
            jcThread.start();

            long temp_counts;
            while(true){
                if(jobControl.allFinished()){
                    //temp_counts = job2.getCounters().findCounter(PageRank.ReachCounter.COUNT ).getValue();
                    //System.out.println(temp_counts);
                    System.out.println(jobControl.getSuccessfulJobList());
                    jobControl.stop();
                    if(jobControl.getFailedJobList().size() > 0){
                        System.out.println(jobControl.getFailedJobList());
                    }
                    break;
                }
            }

        }
  }
