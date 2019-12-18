package kr.re.islab.bigdata.hadoop.publictransportroute.writeable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * GridCoordinateWriteable
 */
public class GridCoordinateWriteable implements WritableComparable<GridCoordinateWriteable> {

    private IntWritable x;
    private IntWritable y;
    private BooleanWritable isPickUp;


    public GridCoordinateWriteable() {
        x = new IntWritable(0);
        y = new IntWritable(0);
        isPickUp = new BooleanWritable(true);
    }

    public GridCoordinateWriteable(IntWritable x, IntWritable y, BooleanWritable isPickUp) {
        this.x = x;
        this.y = y;
        this.isPickUp = isPickUp;
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

    public BooleanWritable getIsPickUp() {
        return this.isPickUp;
    }

    public void setIsPickUp(BooleanWritable isPickUp) {
        this.isPickUp = isPickUp;
    }

    public GridCoordinateWriteable x(IntWritable x) {
        this.x = x;
        return this;
    }

    public GridCoordinateWriteable y(IntWritable y) {
        this.y = y;
        return this;
    }

    public GridCoordinateWriteable isPickUp(BooleanWritable isPickUp) {
        this.isPickUp = isPickUp;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof GridCoordinateWriteable)) {
            return false;
        }
        GridCoordinateWriteable gridCoordinateWriteable = (GridCoordinateWriteable) o;
        return Objects.equals(x, gridCoordinateWriteable.x) && Objects.equals(y, gridCoordinateWriteable.y) && Objects.equals(isPickUp, gridCoordinateWriteable.isPickUp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y, isPickUp);
    }

    @Override
    public String toString() {
        return getX() + "," +  getY() + "," + getIsPickUp() + ",";
    }

  
    @Override
    public void write(DataOutput out) throws IOException {
        x.write(out);
        y.write(out);
        isPickUp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x.readFields(in);
        y.readFields(in);
        isPickUp.readFields(in);
    }

    @Override
    public int compareTo(GridCoordinateWriteable o) {
       int compX = x.compareTo(o.x);
       if(compX == 0) {
        int compY = y.compareTo(o.y);
        if(compY == 0) {
            int compPickup = isPickUp.compareTo(o.isPickUp);
            return compPickup;
        }
        return compY;
       }
       return compX;
    }


}