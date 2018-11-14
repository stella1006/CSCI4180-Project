import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import java.util.StringTokenizer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class PRNodeWritable implements Writable {
    private IntWritable nodeId;
    private DoubleWritable pageRankValue;
    private ArrayList<IntWritable> AdjList;
    private Text text;

    public PRNodeWritable() {
        this.nodeId = new IntWritable(0);
        this.pageRankValue = new DoubleWritable(0.0);
        this.AdjList = new ArrayList<IntWritable>();
        this.text = new Text(this.nodeId.toString() + " " + this.pageRankValue.toString());
    }

    public PRNodeWritable(IntWritable nId) {
        this.nodeId = new IntWritable(nId.get());
        this.pageRankValue = new DoubleWritable(0.0);
        this.AdjList = new ArrayList<IntWritable>();
        this.text = new Text(this.nodeId.toString() + " " + this.pageRankValue.toString());
    }

    public PRNodeWritable(IntWritable nId, DoubleWritable value) {
        this.nodeId = new IntWritable(nId.get());
        this.pageRankValue = new DoubleWritable(value.get());
        this.AdjList = new ArrayList<IntWritable>();
        this.text = new Text(this.nodeId.toString() + " " + this.pageRankValue.toString());
    }

    public PRNodeWritable(String text) {
        this.setFromText(text);
    }

    public void setFromText(String txt) {
        StringTokenizer itr = new StringTokenizer(txt);
        this.nodeId.set(Integer.parseInt(itr.nextToken()));
        this.pageRankValue.set(Double.parseDouble(itr.nextToken()));
        this.text.set(txt);
        while(itr.hasMoreTokens()) {
            AdjList.add(new IntWritable(Integer.parseInt(itr.nextToken())));
        }
    }

    public IntWritable getNodeId() {
        return this.nodeId;
    }
    public DoubleWritable getPageRankValue() {
        return this.pageRankValue;
    }
    public Text getText() {
        //updateText();
        return this.text;
    }
    public ArrayList<IntWritable> getAdjList() {
        return this.AdjList;
    }

    public void setPageRank(DoubleWritable value) {
        String old_page_str = this.nodeId.toString() + " " + this.pageRankValue.toString();
        String old_text = this.text.toString();

        this.pageRankValue.set(value.get());
        String new_page_str = this.nodeId.toString() + " " + value.toString();
        String new_text = old_text.replace(old_page_str, new_page_str);
        this.text.set(new_text);
    }

    public void setText(String text) {
        this.text.set(text);
    }

    public void setNode(PRNodeWritable n2) {
        this.nodeId.set(n2.getNodeId().get());
        this.pageRankValue.set(n2.getPageRankValue().get());
        this.text.set(n2.getText().toString());
        this.AdjList = n2.getAdjList();
        //updateText();
    }

    public void addAdjList(IntWritable item) {
        if (item.get() != this.nodeId.get()) {
            this.AdjList.add(item);
            String temp_str = this.text.toString();
            this.text.set(temp_str+ " " + item.toString());
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        text.readFields(in);
        this.setFromText(this.text.toString());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.text.write(out);
    }

    @Override
	public String toString() {
        return this.text.toString();
    }

}
