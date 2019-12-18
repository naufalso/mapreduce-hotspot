package kr.re.islab.bigdata.hadoop.publictransportroute.constant;
/**
 * TripDataTableConst
 */
public class TripDataTableIndex {

    /*
        = CSV Structure =
        medallion       [0]
        hack_license    [1]
        vendor_id       [2]
        rate_code       [3]
        store_and_fwd_flag  [4]
        pickup_datetime     [5]
        dropoff_datetime    [6]
        passenger_count     [7]
        trip_time_in_secs   [8]
        trip_distance       [9]
        pickup_longitude    [10]
        pickup_latitude     [11]
        dropoff_longitude   [12]
        dropoff_latitude    [13]
    */

    public static final int MEDALLION = 0;
    public static final int HACK_LICENSE = 1;
    public static final int VENDOR_ID = 2;
    public static final int RATE_CODE = 3;
    public static final int STORE_AND_FWD_FLAG = 4;
    public static final int PICKUP_DATETIME = 5;
    public static final int DROPOFF_DATETIME = 6;
    public static final int PESSENGER_COUNT = 7;
    public static final int TRIP_TIME_IN_SECS = 8;
    public static final int TRIP_DISTANCE = 9;
    public static final int PICKUP_LONGITUDE = 10;
    public static final int PICKUP_LATITUDE = 11;
    public static final int DROPOFF_LONGITUDE = 12;
    public static final int DROPOFF_LATITUDE = 13;

    public static final int LENGTH = 14;
    
}