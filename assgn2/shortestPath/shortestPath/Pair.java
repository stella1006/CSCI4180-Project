import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//import java.util.ArrayList;

public class Pair implements Writable {
    private IntWritable first;
    private DoubleWritable second;

    public Pair(IntWritable first, DoubleWritable second) {
        set(first, second);
    }

    public Pair() {
        set(new IntWritable(), new DoubleWritable());
    }

    public IntWritable getFirst() {
        return first;
    }

    public DoubleWritable getSecond() {
        return second;
    }

    public void set(IntWritable first, DoubleWritable second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public String toString() {
        return first + " " + second;
    }
}
