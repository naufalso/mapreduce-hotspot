package kr.re.islab.bigdata.hadoop.publictransportroute.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import kr.re.islab.bigdata.hadoop.publictransportroute.constant.TripDataTableIndex;
import kr.re.islab.bigdata.hadoop.publictransportroute.utils.CoordinateGridHelper;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridCoordinateWriteable;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridValueWriteable;

/**
 * CoordinateMapper
 */
public class CoordinateMapper extends Mapper<Object, Text, GridCoordinateWriteable, GridValueWriteable> {

    private GridCoordinateWriteable gridCoordinate = new GridCoordinateWriteable();
    private GridValueWriteable gridValue = new GridValueWriteable();

    @Override
    protected void map(Object key, Text value,
            Mapper<Object, Text, GridCoordinateWriteable, GridValueWriteable>.Context context)
            throws IOException, InterruptedException {

        String row = value.toString();
        String[] data = row.split(",");

        // CSV column should be 14
        if (data.length != 14) {
            return;
        }

        try {
            Double pickUpLongitude = Double.parseDouble(data[TripDataTableIndex.PICKUP_LONGITUDE]);
            Double pickUpLatitude = Double.parseDouble(data[TripDataTableIndex.PICKUP_LATITUDE]);
            Double dropOffLongitude = Double.parseDouble(data[TripDataTableIndex.DROPOFF_LONGITUDE]);
            Double dropOffLatitude = Double.parseDouble(data[TripDataTableIndex.DROPOFF_LATITUDE]);

            // Filter pickup coordinate to only New York
            if (pickUpLatitude < CoordinateGridHelper.NY_NORTH_LAT && pickUpLatitude > CoordinateGridHelper.NY_SOUTH_LAT
                    && pickUpLongitude < CoordinateGridHelper.NY_EAST_LONG
                    && pickUpLongitude > CoordinateGridHelper.NY_WEST_LONG) {
                CoordinateGridHelper.convertCoordinateToGrid(gridCoordinate, gridValue, pickUpLatitude, pickUpLongitude, (long) 1,
                        true);
                context.write(gridCoordinate, gridValue);
            }

            // Filter dropoff coordinate to only New York
            if (dropOffLatitude < CoordinateGridHelper.NY_NORTH_LAT
                    && dropOffLatitude > CoordinateGridHelper.NY_SOUTH_LAT
                    && dropOffLongitude < CoordinateGridHelper.NY_EAST_LONG
                    && dropOffLongitude > CoordinateGridHelper.NY_WEST_LONG) {
                CoordinateGridHelper.convertCoordinateToGrid(gridCoordinate, gridValue, dropOffLatitude, 
                        dropOffLongitude, (long) 1, false);
                context.write(gridCoordinate, gridValue);
            }
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            e.printStackTrace();
            System.err.println("Error parsing on row : " + row);
        }
    }

}