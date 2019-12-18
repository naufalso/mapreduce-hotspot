package kr.re.islab.bigdata.hadoop.publictransportroute.utils;

import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridCoordinateWriteable;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridValueWriteable;

/**
 * CoordinateGridHelper
 */
public class CoordinateGridHelper {

    public static final Double NY_NORTH_LAT = 40.919324;
    public static final Double NY_SOUTH_LAT = 40.5000168;
    public static final Double NY_WEST_LONG = -74.2612147;
    public static final Double NY_EAST_LONG = -73.7104167;

    public static final Double LAT_GRID = 0.00045884; // about 50m
    public static final Double LONG_GRID = -0.000585955; // about 50m

    public static final Integer MAX_X_GRID = 940;
    public static final Integer MAX_Y_GRID = 920;

    public static void convertCoordinateToGrid(GridCoordinateWriteable gridCoordinate, GridValueWriteable gridValue,
            Double latitude, Double longitude, Long intensity, Boolean isPickup) {
        Double gridY = (NY_NORTH_LAT - latitude) / LAT_GRID;
        Double gridX = (NY_WEST_LONG - longitude) / LONG_GRID;
        if (gridCoordinate == null) {
            gridCoordinate = new GridCoordinateWriteable();
        }

        if (gridValue == null) {
            gridValue = new GridValueWriteable();
        }

        gridCoordinate.getIsPickUp().set(isPickup);
        gridCoordinate.getX().set((int) gridX.doubleValue());
        gridCoordinate.getY().set((int) gridY.doubleValue());

        gridValue.getLatitude().set(latitude);
        gridValue.getLongitude().set(longitude);
        gridValue.getIntensity().set(intensity);
    }
}