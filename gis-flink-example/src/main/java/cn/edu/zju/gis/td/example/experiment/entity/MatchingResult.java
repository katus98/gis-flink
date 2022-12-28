package cn.edu.zju.gis.td.example.experiment.entity;

import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.GlobalUtil;
import lombok.Getter;
import lombok.Setter;
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
    private boolean routeStart;
    /**
     * 是否已经加入到结果流中, 仅对自修正有作用
     */
    private boolean isInStream;

    public MatchingResult(GpsPoint gpsPoint, ResultSet rs) throws SQLException, ParseException, TransformException {
        this.gpsPoint = gpsPoint;
        GeometryFactory factory = JTSFactoryFinder.getGeometryFactory();
        this.originalPoint = (Point) JTS.transform(factory.createPoint(new Coordinate(gpsPoint.getLat(), gpsPoint.getLon())), GlobalConfig.TRANSFORM_G2P);
        this.matchingPoint = (Point) GlobalUtil.WKT_READER.read(rs.getString("cp"));
        this.edgeWithInfo = new EdgeWithInfo(rs);
        this.error = rs.getDouble("dis");
        this.ratioToNextNode = -1;
        this.previousMR = null;
        this.routeStart = false;
        // 是否已经加入到结果流中, 仅对自修正有作用
        this.isInStream = false;
    }

    public MatchingResult(MatchingResult mr) {
        this.originalPoint = mr.getOriginalPoint();
        this.matchingPoint = mr.getMatchingPoint();
        this.gpsPoint = mr.getGpsPoint();
        this.edgeWithInfo = mr.getEdgeWithInfo();
        this.error = mr.getError();
        this.segmentNo = mr.getSegmentNo();
        this.matchingSegment = mr.getMatchingSegment();
        this.ratioToNextNode = mr.getRatioToNextNode();
        this.previousMR = mr.getPreviousMR();
        this.routeStart = mr.isRouteStart();
        this.isInStream = mr.isInStream();
    }

    public static String matchingTitle() {
        return GpsPoint.title() + ",edgeId,oriX,oriY,matX,matY,routeStart";
    }

    public String toMatchingLine() {
        return gpsPoint.toLine() + "," + edgeWithInfo.getId() + "," + originalPoint.getX() + "," + originalPoint.getY()
                + "," + matchingPoint.getX() + "," + matchingPoint.getY() + "," + routeStart;
    }

    public MatPoint toMatPoint() {
        return new MatPoint(this);
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

    @Override
    public String toString() {
        return toMatchingLine();
    }
}
