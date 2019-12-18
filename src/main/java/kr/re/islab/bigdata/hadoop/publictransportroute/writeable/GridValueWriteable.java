package kr.re.islab.bigdata.hadoop.publictransportroute.writeable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * GridValueWriteable
 */
public class GridValueWriteable implements Writable {

    private DoubleWritable latitude;
    private DoubleWritable longitude;
    private LongWritable intensity;


    public GridValueWriteable() {
        latitude = new DoubleWritable();
        longitude = new DoubleWritable();
        intensity = new LongWritable(1);
    }



    public GridValueWriteable(DoubleWritable latitude, DoubleWritable longitude, LongWritable intensity) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.intensity = intensity;
    }

    public DoubleWritable getLatitude() {
        return this.latitude;
    }

    public void setLatitude(DoubleWritable latitude) {
        this.latitude = latitude;
    }

    public DoubleWritable getLongitude() {
        return this.longitude;
    }

    public void setLongitude(DoubleWritable longitude) {
        this.longitude = longitude;
    }

    public LongWritable getIntensity() {
        return this.intensity;
    }

    public void setIntensity(LongWritable intensity) {
        this.intensity = intensity;
    }

    public GridValueWriteable latitude(DoubleWritable latitude) {
        this.latitude = latitude;
        return this;
    }

    public GridValueWriteable longitude(DoubleWritable longitude) {
        this.longitude = longitude;
        return this;
    }

    public GridValueWriteable intensity(LongWritable intensity) {
        this.intensity = intensity;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof GridValueWriteable)) {
            return false;
        }
        GridValueWriteable gridValueWriteable = (GridValueWriteable) o;
        return Objects.equals(latitude, gridValueWriteable.latitude) && Objects.equals(longitude, gridValueWriteable.longitude) && Objects.equals(intensity, gridValueWriteable.intensity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latitude, longitude, intensity);
    }

   
    @Override
    public String toString() {
        return getLatitude() + "," + getLongitude() + "," + getIntensity();
    }
    

    @Override
    public void write(DataOutput out) throws IOException {
        latitude.write(out);
        longitude.write(out);
        intensity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        latitude.readFields(in);
        longitude.readFields(in);
        intensity.readFields(in);
    }



}