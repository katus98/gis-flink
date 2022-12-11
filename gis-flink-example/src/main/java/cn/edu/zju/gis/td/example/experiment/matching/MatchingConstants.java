package cn.edu.zju.gis.td.example.experiment.matching;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-10
 */
public final class MatchingConstants {
    public static final long MAX_DELTA_TIME;
    public static final double MAX_ALLOW_SPEED;
    public static final int GPS_TOLERANCE;
    public static final int MAX_HALF_ROAD_WIDTH;
    public static final int MATCHING_TOLERANCE;
    public static final int CENTER_POINT_NUMBER;

    static {
        MAX_DELTA_TIME = 60000L;
        MAX_ALLOW_SPEED = 50.0;
        GPS_TOLERANCE = 10;
        MAX_HALF_ROAD_WIDTH = 15;
        MATCHING_TOLERANCE = GPS_TOLERANCE + MAX_HALF_ROAD_WIDTH;
        CENTER_POINT_NUMBER = 109306;
    }
}
