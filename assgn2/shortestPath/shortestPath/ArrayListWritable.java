import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import Pair.*;
//import java.util.ArrayList;


public class ArrayListWritable extends ArrayWritable{
	public ArrayListWritable() {
	     super(Pair.class);
	   }
}
