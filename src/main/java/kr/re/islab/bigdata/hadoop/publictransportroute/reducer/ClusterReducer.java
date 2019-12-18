package kr.re.islab.bigdata.hadoop.publictransportroute.reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

// import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.ClusterValueWriteable;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridCoordinateWriteable;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridValueWriteable;

/**
 * ClusterReducer
 */
public class ClusterReducer extends Reducer<GridCoordinateWriteable, ClusterValueWriteable, GridCoordinateWriteable, ClusterValueWriteable>{

    private ClusterValueWriteable clusterValueWriteable = new ClusterValueWriteable();
    private Set<GridValueWriteable> gridValueWriteables = new HashSet<GridValueWriteable>();
    
    @Override
    protected void reduce(GridCoordinateWriteable key, Iterable<ClusterValueWriteable> values,
            Reducer<GridCoordinateWriteable, ClusterValueWriteable, GridCoordinateWriteable, ClusterValueWriteable>.Context context)
            throws IOException, InterruptedException {
                // int x_min = Integer.MAX_VALUE;
                // int x_max = Integer.MIN_VALUE;
                // int y_min = Integer.MAX_VALUE;
                // int y_max = Integer.MIN_VALUE;
                boolean foundSelfGrid = false;
                
                gridValueWriteables.clear();

                for(ClusterValueWriteable clusterValue : values) {
                    // x_min = Math.min(x_min, clusterValue.getX().get());
                    // x_max = Math.max(x_max, clusterValue.getX().get());
                    // y_min = Math.min(y_min, clusterValue.getY().get());
                    // y_max = Math.max(y_max, clusterValue.getY().get());

                    if(clusterValue.getX().get() == key.getX().get() && clusterValue.getY().get() == key.getY().get()) {
                        foundSelfGrid = true;
                    }


                    for(Writable gridValue : clusterValue.getGridValue().get()) {
                        GridValueWriteable grid = (GridValueWriteable) gridValue;
                        gridValueWriteables.add(grid);
                    }
                }

                 if(foundSelfGrid) { // if(gridValueWriteables.size() > 1) {
                    clusterValueWriteable.getX().set(key.getX().get());
                    clusterValueWriteable.getY().set(key.getY().get());
                    clusterValueWriteable.getIsPickup().set(key.getIsPickUp().get());
                    // clusterValueWriteable.getX_min().set(x_min);
                    // clusterValueWriteable.getX_max().set(x_max);
                    // clusterValueWriteable.getY_min().set(y_min);
                    // clusterValueWriteable.getY_max().set(y_max);
                    // clusterValueWriteable.getIsPickup().set(key.getIsPickUp().get());
                    // clusterValueWriteable.getGrid_count().set(gridValueWriteables.size());

                    GridValueWriteable[] gridValueWriteablesArray = new GridValueWriteable[gridValueWriteables.size()];
                    gridValueWriteablesArray = gridValueWriteables.toArray(gridValueWriteablesArray);
                    clusterValueWriteable.getGridValue().set(gridValueWriteablesArray);   
                    context.write(key, clusterValueWriteable);
                }
            
    }

}