package cn.edu.zju.gis.td.example.experiment.entity;

import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.GlobalUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.opengis.referencing.operation.TransformException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-07
 */
@Getter
@Setter
@ToString
public class MatchingResult {
    private Point originalPoint;
    private Point matchingPoint;
    private GpsPoint gpsPoint;
    private EdgeWithInfo edgeWithInfo;
    private double error;
    private int segmentNo;
    private LineString matchingSegment;
    private volatile double ratioToNextNode;
    private MatchingResult previousMR;

    public MatchingResult(GpsPoint gpsPoint, ResultSet rs) throws SQLException, ParseException, TransformException {
        this.gpsPoint = new GpsPoint();
        GeometryFactory factory = JTSFactoryFinder.getGeometryFactory();
        this.originalPoint = (Point) JTS.transform(factory.createPoint(new Coordinate(gpsPoint.getLon(), gpsPoint.getLat())), GlobalConfig.TRANSFORM_G2P);
        this.matchingPoint = (Point) GlobalUtil.WKT_READER.read(rs.getString("cp"));
        this.edgeWithInfo = new EdgeWithInfo(rs);
        this.error = rs.getDouble("dis");
        this.ratioToNextNode = -1;
        this.previousMR = null;
    }

    public void update() {
        if (ratioToNextNode >= 0) {
            return;
        }
        synchronized (this) {
            if (ratioToNextNode < 0) {
                double distance = Integer.MAX_VALUE;
                GeometryFactory factory = JTSFactoryFinder.getGeometryFactory();
                LineString edgeL = edgeWithInfo.getGeometry();
                Coordinate[] coordinates = edgeL.getCoordinates();
                for (int i = 0; i < coordinates.length - 1; i++) {
                    LineString line = factory.createLineString(new Coordinate[]{coordinates[i], coordinates[i + 1]});
                    double dis = line.distance(matchingPoint);
                    if (distance > dis) {
                        distance = dis;
                        this.segmentNo = i;
                        this.matchingSegment = line;
                    }
                }
                Coordinate[] coords = Arrays.copyOfRange(coordinates, segmentNo, coordinates.length);
                coords[0] = matchingPoint.getCoordinate();
                this.ratioToNextNode = factory.createLineString(coords).getLength() / edgeWithInfo.getLength();
            }
        }
    }
}
