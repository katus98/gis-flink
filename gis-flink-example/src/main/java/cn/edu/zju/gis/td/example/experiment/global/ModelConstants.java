package cn.edu.zju.gis.td.example.experiment.global;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-10
 */
public final class ModelConstants {
    public static final long MAX_DELTA_TIME;
    public static final long MAX_FILTER_DELTA_TIME;
    public static final double MAX_ALLOW_SPEED;
    public static final int GPS_TOLERANCE;
    public static final int OSM_TOLERANCE;
    public static final double LANE_WIDTH;
    public static final int MAX_HALF_ROAD_WIDTH;
    public static final int MATCHING_TOLERANCE;
    public static final double DIS_STANDARD_DEVIATION, DIR_STANDARD_DEVIATION;
    public static final int CENTER_POINT_NUMBER;
    public static final double MAX_COST;
    public static final long TIME_WINDOW;

    static {
        MAX_DELTA_TIME = 60000L;
        MAX_FILTER_DELTA_TIME = 3 * MAX_DELTA_TIME;
        MAX_ALLOW_SPEED = 50.0;
        GPS_TOLERANCE = 15;
        OSM_TOLERANCE = 5;
        LANE_WIDTH = 3.75;
        MAX_HALF_ROAD_WIDTH = 15;   // LANE_WIDTH * 4
        MATCHING_TOLERANCE = GPS_TOLERANCE + OSM_TOLERANCE + MAX_HALF_ROAD_WIDTH;
        DIS_STANDARD_DEVIATION = LANE_WIDTH * 2;
        DIR_STANDARD_DEVIATION = 10.0;
        CENTER_POINT_NUMBER = 73715;
        MAX_COST = 1.0 * Integer.MAX_VALUE;
        TIME_WINDOW = 10800000L;
    }
}
