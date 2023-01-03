package cn.edu.zju.gis.td.example.experiment.entity;

import org.locationtech.jts.geom.Point;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-03
 */
public interface Matchable {
    Point getOriginalPoint();
    Point getMatchingPoint();
    EdgeWithInfo getEdgeWithInfo();
    double getRatioToNextNode();
    void update();
}
