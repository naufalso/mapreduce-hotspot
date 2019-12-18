package kr.re.islab.bigdata.hadoop.publictransportroute.writeable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * ClusterValueWriteable
 */
public class ClusterValueWriteableCopy implements Writable {

    private IntWritable x_min;
    private IntWritable y_min;
    private IntWritable x_max;
    private IntWritable y_max;
    private BooleanWritable isPickUp;
    private IntWritable grid_count;
    private ArrayWritable grid_values;



    public ClusterValueWriteableCopy() {
        x_min = new IntWritable();
        y_min = new IntWritable();
        x_max = new IntWritable();
        y_max = new IntWritable();
        isPickUp = new BooleanWritable();
        grid_count = new IntWritable(1);
        grid_values = new ArrayWritable(GridValueWriteable.class);
    }


    public ClusterValueWriteableCopy(IntWritable x_min, IntWritable y_min, IntWritable x_max, IntWritable y_max, BooleanWritable isPickUp, IntWritable grid_count, ArrayWritable grid_values) {
        this.x_min = x_min;
        this.y_min = y_min;
        this.x_max = x_max;
        this.y_max = y_max;
        this.isPickUp = isPickUp;
        this.grid_count = grid_count;
        this.grid_values = grid_values;
    }

    public IntWritable getX_min() {
        return this.x_min;
    }

    public void setX_min(IntWritable x_min) {
        this.x_min = x_min;
    }

    public IntWritable getY_min() {
        return this.y_min;
    }

    public void setY_min(IntWritable y_min) {
        this.y_min = y_min;
    }

    public IntWritable getX_max() {
        return this.x_max;
    }

    public void setX_max(IntWritable x_max) {
        this.x_max = x_max;
    }

    public IntWritable getY_max() {
        return this.y_max;
    }

    public void setY_max(IntWritable y_max) {
        this.y_max = y_max;
    }

    public BooleanWritable getIsPickUp() {
        return this.isPickUp;
    }

    public void setIsPickUp(BooleanWritable isPickUp) {
        this.isPickUp = isPickUp;
    }

    public IntWritable getGrid_count() {
        return this.grid_count;
    }

    public void setGrid_count(IntWritable grid_count) {
        this.grid_count = grid_count;
    }

    public ArrayWritable getGrid_values() {
        return this.grid_values;
    }

    public void setGrid_values(ArrayWritable grid_values) {
        this.grid_values = grid_values;
    }

    public ClusterValueWriteableCopy x_min(IntWritable x_min) {
        this.x_min = x_min;
        return this;
    }

    public ClusterValueWriteableCopy y_min(IntWritable y_min) {
        this.y_min = y_min;
        return this;
    }

    public ClusterValueWriteableCopy x_max(IntWritable x_max) {
        this.x_max = x_max;
        return this;
    }

    public ClusterValueWriteableCopy y_max(IntWritable y_max) {
        this.y_max = y_max;
        return this;
    }

    public ClusterValueWriteableCopy isPickUp(BooleanWritable isPickUp) {
        this.isPickUp = isPickUp;
        return this;
    }

    public ClusterValueWriteableCopy grid_count(IntWritable grid_count) {
        this.grid_count = grid_count;
        return this;
    }

    public ClusterValueWriteableCopy grid_values(ArrayWritable grid_values) {
        this.grid_values = grid_values;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof ClusterValueWriteable)) {
            return false;
        }
        ClusterValueWriteableCopy clusterValueWriteable = (ClusterValueWriteableCopy) o;
        return Objects.equals(x_min, clusterValueWriteable.x_min) && Objects.equals(y_min, clusterValueWriteable.y_min) && Objects.equals(x_max, clusterValueWriteable.x_max) && Objects.equals(y_max, clusterValueWriteable.y_max) && Objects.equals(isPickUp, clusterValueWriteable.isPickUp) && Objects.equals(grid_count, clusterValueWriteable.grid_count) && Objects.equals(grid_values, clusterValueWriteable.grid_values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(x_min, y_min, x_max, y_max, isPickUp, grid_count, grid_values);
    }


    @Override
    public String toString() {
        GridValueWriteable[] gridValueWriteables = (GridValueWriteable[]) getGrid_values().get();
        String gridValueWriteablesString = "[";
        for (GridValueWriteable gridValueWriteable : gridValueWriteables) {
            gridValueWriteablesString += gridValueWriteable.toString() + ",";
        }
        gridValueWriteablesString+="]";
        return getX_min() + "," + getY_min() + "," + getX_max() + "," + getY_max() + "," + getIsPickUp() + "," + getGrid_count() + "," + gridValueWriteablesString;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        x_min.write(out);
        y_min.write(out);
        x_max.write(out);
        y_max.write(out);
        isPickUp.write(out);
        grid_values.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x_min.readFields(in);
        y_min.readFields(in);
        x_max.readFields(in);
        y_max.readFields(in);
        isPickUp.readFields(in);
        grid_values.readFields(in);
    }

    
}