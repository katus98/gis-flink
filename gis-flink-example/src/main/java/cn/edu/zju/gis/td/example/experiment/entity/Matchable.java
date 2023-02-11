package cn.edu.zju.gis.td.example.experiment.entity;

import org.locationtech.jts.geom.Point;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-03
 */
public interface Matchable {
    long getId();
    Point getOriginalPoint();
    Point getMatchingPoint();
    Edge getEdge();
    double getRatioToNextNode();
    void update();
    boolean isRouteStart();
    long getTimestamp();
    double getMatX();
    double getMatY();
    int getTaxiId();
}
