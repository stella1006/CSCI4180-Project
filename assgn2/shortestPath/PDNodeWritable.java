
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class PDNodeWritable implements Writable {
    private IntWritable nodeId;
    private IntWritable distance;
    private IntWritable prevId;
    private ArrayList<ArrayList<IntWritable>> AdjList;
    private Text text;

    public PDNodeWritable() {
        this.nodeId = new IntWritable(0);
        this.distance = new IntWritable((Integer.MAX_VALUE));
        this.prevId = new IntWritable(0);
        this.AdjList = new ArrayList<ArrayList<IntWritable>>();
        this.text = new Text(this.nodeId.toString() + " " + this.prevId.toString() + " " + this.distance.toString() + ";");
    }

    public PDNodeWritable(IntWritable nodeId) {
        this.nodeId = nodeId;
        this.prevId = new IntWritable(0);
        this.distance = new IntWritable((Integer.MAX_VALUE));
        this.AdjList = new ArrayList<ArrayList<IntWritable>>();
        this.text = new Text(this.nodeId.toString() + " " + this.prevId.toString() + " " + this.distance.toString() + ";");
    }

    public PDNodeWritable(IntWritable nodeId, IntWritable distance) {
        this.nodeId = nodeId;
        this.distance = distance;
        this.prevId = new IntWritable(0);
        this.AdjList = new ArrayList<ArrayList<IntWritable>>();
        this.text = new Text(this.nodeId.toString() + " " + this.prevId.toString() + " " + this.distance.toString() + ";");
    }

    public PDNodeWritable(IntWritable nodeId, IntWritable distance, IntWritable prevId) {
        this.nodeId = nodeId;
        this.distance = distance;
        this.prevId = prevId;
        this.AdjList = new ArrayList<ArrayList<IntWritable>>();
        this.text = new Text(this.nodeId.toString() + " " + this.prevId.toString() + " " + this.distance.toString() + ";" );
    }

    public PDNodeWritable(String text) {
        this.setFromText(text);
    }

    public void setFromText(String txt) {
        String adj[] = txt.toString().split("[^0-9]+");
        //String adj[] = list[1].split("[^1-9]");
        this.nodeId.set(Integer.parseInt(adj[0]));
        this.prevId.set(Integer.parseInt(adj[1]));
        this.distance.set(Integer.parseInt(adj[2]));
        this.text.set(txt);
        //if (text.toString().charAt(0) == '-') this.nodeId.set(this.nodeId.get()*(-1));
        if (adj.length > 4) {
            this.AdjList = new ArrayList<ArrayList<IntWritable>>();
            for (int i = 3; i < adj.length; i+=2) {
                if (Integer.parseInt(adj[i]) != nodeId.get()) {
                    ArrayList<IntWritable> temp = new ArrayList<IntWritable>();
                    temp.add(new IntWritable(Integer.parseInt(adj[i])));
                    temp.add(new IntWritable(Integer.parseInt(adj[i+1])));
                    this.AdjList.add(temp);
                }

            }
        }

    }
    public IntWritable getNodeId() {
        return this.nodeId;
    }
    public IntWritable getDistance() {
        return this.distance;
    }
    public IntWritable getPrevId() {
        return this.prevId;
    }
    public Text getText() {
        updateText();
        return this.text;
    }
    public ArrayList<ArrayList<IntWritable>> getAdjList() {
        return this.AdjList;
    }

    // public void setNodeId(IntWritable nodeId) {
    //     this.nodeId.set(nodeId.get());
    //     //this.text.set(nodeId.toString() + " " + distance.toString() + ";");
    // }
    public void setDistance(IntWritable dis) {
        String old_dis_str = " " + this.distance.toString() + ";";
        String old_text = this.text.toString();

        this.distance.set(dis.get());
        String new_dis_str = " " + dis.toString() + ";";
        String new_text = old_text.replace(old_dis_str, new_dis_str);
        this.text.set(new_text);
    }

    public void setText(String text) {
        this.text.set(text);
    }

    public void updateText() {
        String temp = this.nodeId.toString() + " " + this.prevId.toString() + " " + this.distance.toString() + ";";
        for (int i = 0; i < this.AdjList.size(); i++) {
            if (i != 0) temp+=",";
            temp+="(" + this.AdjList.get(i).get(0).toString() + " " + this.AdjList.get(i).get(1).toString()+ ")";
        }
        //Text tmp_text = new Text(temp);
        this.text.set(temp);
    }

    public void setNode(PDNodeWritable n2) {
        this.nodeId.set(n2.getNodeId().get());
        this.distance.set(n2.getDistance().get());
        this.prevId.set(n2.getPrevId().get());
        this.text.set(n2.getText().toString());
        this.AdjList = n2.getAdjList();
        //updateText();
    }

    public void addAdjList(ArrayList<IntWritable> item) {
        this.AdjList.add(item);
        String temp_str = this.text.toString();
        if (temp_str.charAt(temp_str.length()-1) != ';') temp_str+= ",";
        this.text.set(temp_str+ "(" + item.get(0).toString() + " " + item.get(1).toString() + ")");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //nodeId = new IntWritable(in.readInt());
        //nodeId.readFields(in);
        text.readFields(in);
        this.setFromText(this.text.toString());
        // private Text str_tmp = new Text("");
        // str_tmp.readFields(in);
		//field2.readFields(in);(in);
        //distance = in.readInt();
        // String str = in.readUTF();

        // String str = str_tmp.toString();
        // String[] list=str.split(",");
        // for (String item : list) {
        //     String[] pairs = item.split("[^1-9]+");
        //     ArrayList<IntWritable> temp = new ArrayList<IntWritable>();
        //     temp.add(new IntWritable(Integer.parseInt(pairs[0])));
        //     temp.add(new IntWritable(Integer.parseInt(pairs[1])));
        //     AdjList.add(temp);
        // }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //out.writeInt(nodeId);
        //nodeId.write(out);
        this.text.write(out);
        //out.writeInt(distance);
        //out.writeInt(ListLength);
        // String temp="";
        // for (int i = 0; i < AdjList.size(); i++) {
        //     if (i != 0) temp+=",";
        //     temp+="(" + AdjList.get(i).get(0).toString() + " " + AdjList.get(i).get(1).toString()+ ")";
        // }
        // Text tmp_text = new Text(temp);
        // Text.write(out);
    }

    @Override
	public String toString() {
        //Text result = this.getText();
        return this.text.toString();
        // if (distance.get() == Integer.MAX_VALUE) {
        //     return text.toString();
        // }
        // else {
        //     return "(" + nodeId.toString() + "," + distance.toString() + ")";
        // }
	}

    // public static PDNodeWritable read(DataInput in) throws IOException {
    //      PDNodeWritable w = new PDNodeWritable();
    //      w.readFields(in);
    //      return w;
    //    }
}
