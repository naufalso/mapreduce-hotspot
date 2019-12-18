package kr.re.islab.bigdata.hadoop.publictransportroute.reducer;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridCoordinateWriteable;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridValueWriteable;

/**
 * HotSpotReducer
 */
public class HotSpotReducer extends Reducer<GridCoordinateWriteable, GridValueWriteable, GridCoordinateWriteable, GridValueWriteable>{

    private GridValueWriteable result = new GridValueWriteable();

    private GridValueWriteable filterAndCorrectCenterGridCoordinate(int x, int y, double latitude, double longitude, long intensity, GridValueWriteable gridVal) {
        // double gridN = CoordinateMapper.NY_NORTH_LAT - CoordinateMapper.LAT_GRID * ( y - 1);
        // double gridS = CoordinateMapper.NY_NORTH_LAT - CoordinateMapper.LAT_GRID * y;
        // double gridE = CoordinateMapper.NY_WEST_LONG - CoordinateMapper.LONG_GRID * ( x - 1);
        // double gridW = CoordinateMapper.NY_WEST_LONG - CoordinateMapper.LONG_GRID * x;
        
        // Center is in correct grid
        // if(latitude <= gridN && latitude >= gridS && longitude <= gridE && longitude
        // >= gridW ) {
        // gridVal.getLatitude().set(latitude);
        // gridVal.getLongitude().set(longitude);
        // } else { // Center is not in correct grid just return the middle of grid
        // gridVal.getLatitude().set((gridN + gridS) / 2);
        // gridVal.getLongitude().set(gridE + gridW / 2);
        // }

        gridVal.getIntensity().set(intensity);
        gridVal.getLatitude().set(latitude);
        gridVal.getLongitude().set(longitude);
        return gridVal;
    }

    @Override
    protected void reduce(GridCoordinateWriteable key, Iterable<GridValueWriteable> values,
            Reducer<GridCoordinateWriteable, GridValueWriteable, GridCoordinateWriteable, GridValueWriteable>.Context context)
            throws IOException, InterruptedException {
        long intensity = 0;
        double latSum = 0.0;
        double longSum = 0.0;

        for(GridValueWriteable gridValue : values) {
            intensity++;
            latSum += gridValue.getLatitude().get();
            longSum += gridValue.getLongitude().get();
        }

        // Find the center of grid based on intensity
        double latAvg = latSum / intensity;
        double longAvg = longSum / intensity;

        filterAndCorrectCenterGridCoordinate(key.getX().get(), key.getY().get(), latAvg, longAvg, intensity, result);

        context.write(key, result);

    }
    
}