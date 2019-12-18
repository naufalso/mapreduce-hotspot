package kr.re.islab.bigdata.hadoop.publictransportroute.reducer;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridCoordinateWriteable;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridValueWriteable;

/**
 * ClusterCenterReducer
 */
public class ClusterCenterReducer
        extends Reducer<GridCoordinateWriteable, GridValueWriteable, GridCoordinateWriteable, GridValueWriteable> {

    private GridValueWriteable result = new GridValueWriteable();

    @Override
    protected void reduce(GridCoordinateWriteable key, Iterable<GridValueWriteable> values,
            Reducer<GridCoordinateWriteable, GridValueWriteable, GridCoordinateWriteable, GridValueWriteable>.Context context)
            throws InterruptedException, IOException {
        // long maxIntensity = 0;
        // double latMax = 0.0;
        // double longMax = 0.0;

        for (GridValueWriteable gridValue : values) {

            result.getIntensity().set(gridValue.getIntensity().get());
            result.getLatitude().set(gridValue.getLatitude().get());
            result.getLongitude().set(gridValue.getLongitude().get());

            context.write(key, result);
            return;

            // System.out.println(gridValue.toString());

            // if(gridValue.getIntensity().get() > maxIntensity) {
            //     latMax = gridValue.getLatitude().get();
            //     longMax = gridValue.getLongitude().get();
            //     maxIntensity = gridValue.getIntensity().get();
            // }
        }

        // result.getIntensity().set(maxIntensity);
        // result.getLatitude().set(latMax);
        // result.getLongitude().set(longMax);

        // context.write(key, result);

    }
}