
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;


public class PDNodeWritable implements Writable {
    private Integer nodeId;
    private Integer distance;
    private ArrayList<ArrayList<Integer>> AdjList;

    public PDNodeWritable() {
        this.nodeId = 0;
        this.distance = Integer.MAX_VALUE;
        this.AdjList = new ArrayList<ArrayList<Integer>>();
    }

    public PDNodeWritable(Integer nodeId) {
        this.nodeId = nodeId;
        this.distance = Integer.MAX_VALUE;
        this.AdjList = new ArrayList<ArrayList<Integer>>();
    }


    public PDNodeWritable(Integer nodeId, Integer distance) {
        this.nodeId = nodeId;
        this.distance = distance;
        this.AdjList = new ArrayList<ArrayList<Integer>>();
    }

    public Integer getNodeId() {
        return nodeId;
    }
    public Integer getDistance() {
        return distance;
    }
    public ArrayList<ArrayList<Integer>> getAdjList() {
        return AdjList;
    }

    public void setNodeId(Integer nodeId) {
        this.nodeId = nodeId;
    }
    public void setDistance(Integer distance) {
        this.distance = distance;
    }
    public void addAdjList(ArrayList<Integer> item) {
        this.AdjList.add(item);
    }

    public void readFields(DataInput in) throws IOException {
        nodeId = in.readInt();
        //distance = in.readInt();
        String str = in.readUTF();
        String[] list=str.split(",");
        for (String item : list) {
            String[] pairs = item.split("[^1-9]+");
            ArrayList<Integer> temp = new ArrayList<Integer>();
            temp.add(Integer.parseInt(pairs[0]));
            temp.add(Integer.parseInt(pairs[1]));
            AdjList.add(temp);
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(nodeId);
        //out.writeInt(distance);
        //out.writeInt(ListLength);
        String temp="";
        for (int i = 0; i < AdjList.size(); i++) {
            if (i != 0) temp+=",";
            temp+="(" + Integer.toString(AdjList.get(i).get(0)) + " " + Integer.toString(AdjList.get(i).get(1)) + ")";
        }
        out.writeUTF(temp);
    }
}
