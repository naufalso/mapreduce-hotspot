package kr.re.islab.bigdata.hadoop.publictransportroute.writeable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * ClusterValueWriteable
 */
public class ClusterValueWriteable implements Writable {

    private IntWritable x;
    private IntWritable y;
    private BooleanWritable isPickup;
    private ArrayWritable gridValue;
    private Text detail;

    public ClusterValueWriteable() {
        x = new IntWritable();
        y = new IntWritable();
        isPickup = new BooleanWritable();
        gridValue = new ArrayWritable(GridValueWriteable.class);
        detail = new Text();
    }


    public ClusterValueWriteable(IntWritable x, IntWritable y, BooleanWritable isPickup, ArrayWritable gridValue) {
        this.x = x;
        this.y = y;
        this.isPickup = isPickup;
        this.gridValue = gridValue;
    }


    public ClusterValueWriteable(IntWritable x, IntWritable y, BooleanWritable isPickup, ArrayWritable gridValue, Text detail) {
        this.x = x;
        this.y = y;
        this.isPickup = isPickup;
        this.gridValue = gridValue;
        this.detail = detail;
    }

    public IntWritable getX() {
        return this.x;
    }

    public void setX(IntWritable x) {
        this.x = x;
    }

    public IntWritable getY() {
        return this.y;
    }

    public void setY(IntWritable y) {
        this.y = y;
    }

    public BooleanWritable getIsPickup() {
        return this.isPickup;
    }

    public void setIsPickup(BooleanWritable isPickup) {
        this.isPickup = isPickup;
    }

    public ArrayWritable getGridValue() {
        return this.gridValue;
    }

    public void setGridValue(ArrayWritable gridValue) {
        this.gridValue = gridValue;
    }

    public Text getDetail() {
        return this.detail;
    }

    public void setDetail(Text detail) {
        this.detail = detail;
    }

    public ClusterValueWriteable x(IntWritable x) {
        this.x = x;
        return this;
    }

    public ClusterValueWriteable y(IntWritable y) {
        this.y = y;
        return this;
    }

    public ClusterValueWriteable isPickup(BooleanWritable isPickup) {
        this.isPickup = isPickup;
        return this;
    }

    public ClusterValueWriteable gridValue(ArrayWritable gridValue) {
        this.gridValue = gridValue;
        return this;
    }

    public ClusterValueWriteable detail(Text detail) {
        this.detail = detail;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof ClusterValueWriteable)) {
            return false;
        }
        ClusterValueWriteable clusterValueWriteable = (ClusterValueWriteable) o;
        return Objects.equals(x, clusterValueWriteable.x) && Objects.equals(y, clusterValueWriteable.y) && Objects.equals(isPickup, clusterValueWriteable.isPickup) && Objects.equals(gridValue, clusterValueWriteable.gridValue) && Objects.equals(detail, clusterValueWriteable.detail);
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y, isPickup, gridValue, detail);
    }
  

  
    @Override
    public String toString() {
        GridValueWriteable[] gridValueWriteables = (GridValueWriteable[]) getGridValue().get();
        String gridValueWriteablesString = ""; // "["
        for (GridValueWriteable gridValueWriteable : gridValueWriteables) {
            gridValueWriteablesString += gridValueWriteable.toString() + "|";
        }
        //gridValueWriteablesString+="]";
        return gridValueWriteablesString;
        // return getX() + "," + getY() + "," + getIsPickup() + "," + gridValueWriteablesString + "," + getDetail();
    }


    @Override
    public void write(DataOutput out) throws IOException {
        x.write(out);
        y.write(out);
        isPickup.write(out);
        gridValue.write(out);
        detail.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x.readFields(in);
        y.readFields(in);
        isPickup.readFields(in);
        gridValue.readFields(in);
        detail.readFields(in);
    }

    
}