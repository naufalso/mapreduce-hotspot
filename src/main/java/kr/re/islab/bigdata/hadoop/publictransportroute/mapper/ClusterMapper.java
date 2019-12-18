
package kr.re.islab.bigdata.hadoop.publictransportroute.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import kr.re.islab.bigdata.hadoop.publictransportroute.constant.GridCoordinateIndex;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.ClusterValueWriteable;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridCoordinateWriteable;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridValueWriteable;

/**
 * ClusterMapper
 */
public class ClusterMapper extends Mapper<Object, Text, GridCoordinateWriteable, ClusterValueWriteable>{

    private GridCoordinateWriteable gridCoordinate = new GridCoordinateWriteable();
    private ClusterValueWriteable clusterValueWriteable = new ClusterValueWriteable();
    private DoubleWritable xCenter = new DoubleWritable();
    private DoubleWritable yCenter = new DoubleWritable();
    private LongWritable intensity = new LongWritable();
    private List<GridValueWriteable> gridValueWriteables = new ArrayList<GridValueWriteable>();

    // private Text textKey = new Text();

    @Override
    protected void map(Object key, Text value,
            Mapper<Object, Text, GridCoordinateWriteable, ClusterValueWriteable>.Context context)
            throws IOException, InterruptedException {
        String row = value.toString();
        if(row.contains("|")) {
            String keyValue[] = row.split("\t");
            String keyString[] = keyValue[0].split(",");
            String valueString[] = keyValue[1].split("\\|");

            int xGrid = Integer.parseInt(keyString[GridCoordinateIndex.X_GRID]);
            int yGrid = Integer.parseInt(keyString[GridCoordinateIndex.Y_GRID]);
            boolean isPickUp = Boolean.parseBoolean(keyString[GridCoordinateIndex.IS_PICKUP]);

            gridValueWriteables.clear();
            
            for(String center: valueString) {
                if(center.length() > 0) {
                    String centerGrid[] = center.split(",");
                    if(centerGrid.length == 3) {
                        gridValueWriteables
                                .add(new GridValueWriteable(new DoubleWritable(Double.parseDouble(centerGrid[0])),
                                        new DoubleWritable(Double.parseDouble(centerGrid[1])),
                                        new LongWritable(Long.parseLong(centerGrid[2]))));
                        // System.out.println("centerGrid Length : " + centerGrid.length);              
                    }  else {
                        System.out.println("ERROR -> centerGrid Length : " + centerGrid.length + " | " + center);
                    }
                }
                //System.out.println("center : " + center);
            }
            GridValueWriteable[] gridValueWriteablesArray = new GridValueWriteable[gridValueWriteables.size()];
            gridValueWriteablesArray = gridValueWriteables.toArray(gridValueWriteablesArray);
            clusterValueWriteable.getGridValue().set(gridValueWriteablesArray);

            gridCoordinate.getIsPickUp().set(isPickUp);
            clusterValueWriteable.getX().set(xGrid);
            clusterValueWriteable.getY().set(yGrid);
            clusterValueWriteable.getIsPickup().set(isPickUp);
            // Emit to all neighbour
            for (int x = -1; x <= 1; x++) {
                for (int y = -1; y <= 1; y++) {

                    gridCoordinate.getX().set(xGrid + x);
                    gridCoordinate.getY().set(yGrid + y);
                    context.write(gridCoordinate, clusterValueWriteable);
                }
            }

        } else {
            String[] data = row.replaceAll("\t", "").split(",");

            int xGrid = Integer.parseInt(data[GridCoordinateIndex.X_GRID]);
            int yGrid = Integer.parseInt(data[GridCoordinateIndex.Y_GRID]);
            boolean isPickUp = Boolean.parseBoolean(data[GridCoordinateIndex.IS_PICKUP]);

            xCenter.set(Double.parseDouble(data[GridCoordinateIndex.X_CENTER]));
            yCenter.set(Double.parseDouble(data[GridCoordinateIndex.Y_CENTER]));
            intensity.set(Long.parseLong(data[GridCoordinateIndex.INTENSITY]));

            if (intensity.get() < 30) {
                return;
            }
            gridValueWriteables.clear();
            gridValueWriteables.add(new GridValueWriteable(xCenter, yCenter, intensity));
            GridValueWriteable[] gridValueWriteablesArray = new GridValueWriteable[gridValueWriteables.size()];
            gridValueWriteablesArray = gridValueWriteables.toArray(gridValueWriteablesArray);
            clusterValueWriteable.getGridValue().set(gridValueWriteablesArray);
            gridCoordinate.getIsPickUp().set(isPickUp);

            clusterValueWriteable.getX().set(xGrid);
            clusterValueWriteable.getY().set(yGrid);
            clusterValueWriteable.getIsPickup().set(isPickUp);
            // Emit to all neighbour
            for (int x = -1; x <= 1; x++) {
                for (int y = -1; y <= 1; y++) {

                    gridCoordinate.getX().set(xGrid + x);
                    gridCoordinate.getY().set(yGrid + y);
                    
                    // clusterValueWriteable.getX_min().set(Math.min(xGrid + x, xGrid));
                    // clusterValueWriteable.getX_max().set(Math.max(xGrid + x, xGrid));
                    // clusterValueWriteable.getY_min().set(Math.min(yGrid + y, yGrid));
                    // clusterValueWriteable.getY_max().set(Math.max(yGrid + y, yGrid));

                    // textKey.set(gridCoordinate.getX().get() + "," + gridCoordinate.getY().get() +
                    // ","
                    // + gridCoordinate.getIsPickUp().get()); //x_min, y_min, x_max, y_max,
                    // isPickUp,

                    context.write(gridCoordinate, clusterValueWriteable);
                }
            }
        }
    }
}