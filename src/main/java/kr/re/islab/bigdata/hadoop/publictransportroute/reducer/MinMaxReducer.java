package kr.re.islab.bigdata.hadoop.publictransportroute.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import kr.re.islab.bigdata.hadoop.publictransportroute.constant.TripDataTableIndex;
/**
 * MinMaxReducer
 */
public class MinMaxReducer {
    public static class CoordinateMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text pdo = new Text();
        private DoubleWritable coordinate = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {
            String row = value.toString();
            String[] data = row.split(",");

            try {
                Double pickUpLongitude = Double.parseDouble(data[TripDataTableIndex.PICKUP_LONGITUDE]);
                Double pickUpLatitude = Double.parseDouble(data[TripDataTableIndex.PICKUP_LATITUDE]);
                Double dropOffLongitude = Double.parseDouble(data[TripDataTableIndex.DROPOFF_LONGITUDE]);
                Double dropOffLatitude = Double.parseDouble(data[TripDataTableIndex.DROPOFF_LATITUDE]);

                if (pickUpLongitude != 0 && pickUpLatitude != 0 && dropOffLongitude != 0 && dropOffLatitude != 0
                        && pickUpLongitude > -75.0 && pickUpLongitude < 73.0 && dropOffLongitude > -75.0
                        && dropOffLongitude < 73.0 && pickUpLatitude > 39.0 && pickUpLatitude < 42.0
                        && dropOffLatitude > 39.0 && dropOffLatitude < 42.0) {
                    pdo.set("pickUpLongitude");
                    coordinate.set(pickUpLongitude);
                    context.write(pdo, coordinate);

                    pdo.set("pickUpLatitude");
                    coordinate.set(pickUpLatitude);
                    context.write(pdo, coordinate);

                    pdo.set("dropOffLongitude");
                    coordinate.set(dropOffLongitude);
                    context.write(pdo, coordinate);

                    pdo.set("dropOffLatitude");
                    coordinate.set(dropOffLatitude);
                    context.write(pdo, coordinate);
                }
            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                e.printStackTrace();
                System.err.println("Error parsing on row : " + row);
            }

        }
    }

    public static class MinMaxReducers extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values,
                Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {
            double minCoordinate = Double.MAX_VALUE;
            double maxCoordinate = Double.MIN_VALUE;

            for (DoubleWritable coordinate : values) {
                double coordinateVal = coordinate.get();
                if (coordinateVal < minCoordinate) {
                    minCoordinate = coordinateVal;
                }
                if (coordinateVal > maxCoordinate) {
                    maxCoordinate = coordinateVal;
                }
            }

            result.set(maxCoordinate);
            context.write(key, result);
        }
    }
    
}