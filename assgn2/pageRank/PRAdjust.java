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

//
// class PRNodeecordReader extends RecordReader<IntWritable,PRNodeWritable>  {
//     LineRecordReader lineRecordReader;
//     IntWritable key;
//     PRNodeWritable value;
//
//     @Override
//     public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
//         lineRecordReader = new LineRecordReader();
//         lineRecordReader.initialize(inputSplit, context);
//     }
//
//     // It’s the method that will be used to transform the line into key value
//     @Override
//     public boolean nextKeyValue() throws IOException, InterruptedException {
//         if (!lineRecordReader.nextKeyValue()) {
//             return false;
//         }
//         String line = new String(lineRecordReader.getCurrentValue().toString());
//         //String[] keyValue = line.split("\t");
//         //String[] keyFields = keyValue[0].split(" ");
//         //valueFields = keyValue[1].split(" ");
//         //IntWritable temp = new IntWritable(line);
//         value = new PRNodeWritable();
//         //key.setText(line);
//         value.setFromText(line);
//         key = value.getNodeId();
//         //IntWritable nodeId = temp.getNodeId();
//         //key = new PRNodeWritable(nodeId);
//         //value = new PRNodeWritable(IntWritable);
//         return true;
//     }
//
//     @Override
//         public IntWritable getCurrentKey() throws IOException, InterruptedException {
//             return key;
//         }
//
//         @Override
//         public PRNodeWritable getCurrentValue() throws IOException, InterruptedException {
//             return value;
//         }
//
//         @Override
//         public float getProgress() throws IOException, InterruptedException {
//             return 0;
//         }
//
//         @Override
//         public void close() throws IOException {
//             lineRecordReader.close();
//         }
// }
//
// class PRNodeInputFormat extends FileInputFormat<IntWritable,PRNodeWritable> {
//       @Override
//       public RecordReader<IntWritable,PRNodeWritable> createRecordReader(InputSplit inputSplit,TaskAttemptContext context)
//           throws IOException, InterruptedException {
//
//           PRNodeecordReader prRecordReader = new PRNodeecordReader();
//           prRecordReader.initialize(inputSplit, context);
//           return prRecordReader;
//       }
//   }

public class PRAdjust extends Mapper<IntWritable, PRNodeWritable, NullWritable, PRNodeWritable>{
    public Integer NodeCounter = 0;
    public Double mm = 0.0;
    ArrayList<PRNodeWritable> list = new ArrayList<PRNodeWritable>();
    public void map(IntWritable key, PRNodeWritable value, Context context
            ) throws IOException, InterruptedException {
                NodeCounter++;
                mm += value.getPageRankValue().get();
                list.add(value);
                //context.write(value.getNodeId(), value);
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Double alpha = Double.parseDouble(conf.get("alpha"));
        Double threshold = Double.parseDouble(conf.get("threshold"));

        for (PRNodeWritable val : list) {
            Double newPageRank = alpha*(1.0/NodeCounter) +
                                (1-alpha)*((1-mm)/NodeCounter + val.getPageRankValue().get());
            val.setPageRank(new DoubleWritable(newPageRank));
            if (val.getPageRankValue().get() > threshold) {
                context.write(NullWritable.get(), val);
            }

        }
        //context.write(new IntWritable(0), new PRNodeWritable(new IntWritable(0), new DoubleWritable(mm)));
    }
    }

        // public static class PRAdjustReducer
        //       extends Reducer<IntWritable,PRNodeWritable,IntWritable,PRNodeWritable> {
        //
        //           public void reduce(IntWritable key, Iterable<PRNodeWritable> values,
        //                   Context context
        //                   ) throws IOException, InterruptedException {
        //                       // for (PRNodeWritable val : values) {
        //                       //     context.write(key, val);
        //                       // }
        //                       Configuration conf = context.getConfiguration();
        //                       Double alpha = Double.parseDouble(conf.get("alpha"));
        //
        //                       for (PRNodeWritable val : values) {
        //                           Double newPageRank = alpha*(1.0/NodeCounter) +
        //                                             (1-alpha)*((1-mm)/NodeCounter + val.getPageRankValue().get());
        //                           val.setPageRank(new DoubleWritable(newPageRank));
        //                           context.write(key, val);
        //                       }
        //                   }
        //       }
  // }
