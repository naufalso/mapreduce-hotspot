package kr.re.islab.bigdata.hadoop.publictransportroute.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import kr.re.islab.bigdata.hadoop.publictransportroute.constant.GridCoordinateIndex;
import kr.re.islab.bigdata.hadoop.publictransportroute.utils.CoordinateGridHelper;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridCoordinateWriteable;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridValueWriteable;

/**
 * ClusterCentererMapper
 */
public class ClusterCentererMapper extends Mapper<Object, Text, GridCoordinateWriteable, GridValueWriteable> {

    private GridCoordinateWriteable gridCoordinate = new GridCoordinateWriteable();
    private GridValueWriteable gridValue = new GridValueWriteable();
    private List<GridValueWriteable> gridValueWriteables = new ArrayList<GridValueWriteable>();

    @Override
    protected void map(Object key, Text value,
            Mapper<Object, Text, GridCoordinateWriteable, GridValueWriteable>.Context context)
            throws IOException, InterruptedException {
        String row = value.toString();
        if (row.contains("|")) {
            String keyValue[] = row.split("\t");
            String keyString[] = keyValue[0].split(",");
            String valueString[] = keyValue[1].split("\\|");

            boolean isPickUp = Boolean.parseBoolean(keyString[GridCoordinateIndex.IS_PICKUP]);
            gridValueWriteables.clear();

            for (String center : valueString) {
                if (center.length() > 0) {
                    String centerGrid[] = center.split(",");
                    if (centerGrid.length == 3) {
                        gridValueWriteables
                                .add(new GridValueWriteable(new DoubleWritable(Double.parseDouble(centerGrid[0])),
                                        new DoubleWritable(Double.parseDouble(centerGrid[1])),
                                        new LongWritable(Long.parseLong(centerGrid[2]))));
                        // System.out.println("centerGrid Length : " + centerGrid.length);
                    } else {
                        System.out.println("ERROR -> centerGrid Length : " + centerGrid.length + " | " + center);
                    }
                }
                // System.out.println("center : " + center);
            }

            long totalIntensity = 0;
            double totalLongitude = 0;
            double totalLatitiude = 0;
            for(GridValueWriteable gridValueWriteable : gridValueWriteables) {
                totalIntensity += gridValueWriteable.getIntensity().get();
                totalLongitude += (gridValueWriteable.getLongitude().get() * gridValueWriteable.getIntensity().get());
                totalLatitiude += (gridValueWriteable.getLatitude().get() * gridValueWriteable.getIntensity().get());
            }

            double avgLongitude = totalLongitude / totalIntensity;
            double avgLatitude = totalLatitiude / totalIntensity;

            CoordinateGridHelper.convertCoordinateToGrid(gridCoordinate, gridValue, avgLatitude, 
                    avgLongitude, totalIntensity, isPickUp);

            System.out.println("Mapper Out -> " + gridCoordinate.toString() + " ~ " + gridValue.toString());
            context.write(gridCoordinate, gridValue);
        }
    }
    
}