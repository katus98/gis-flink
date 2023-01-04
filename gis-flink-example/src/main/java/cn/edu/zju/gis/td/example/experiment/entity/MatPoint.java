package cn.edu.zju.gis.td.example.experiment.entity;

import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;

import java.sql.SQLException;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-24
 */
@Getter
@Setter
@Slf4j
public class MatPoint implements Matchable {
    private long id;
    private int taxiId;
    private double oriX;
    private double oriY;
    private double matX;
    private double matY;
    private long edgeId;
    private double ratioToNextNode;
    private boolean routeStart;
    private long timestamp;
    private double speed;
    private volatile Edge edge;

    public MatPoint(MatchingResult mr) {
        this.id = mr.getGpsPoint().getId();
        this.taxiId = mr.getGpsPoint().getTaxiId();
        this.oriX = mr.getOriginalPoint().getX();
        this.oriY = mr.getOriginalPoint().getY();
        this.matX = mr.getMatchingPoint().getX();
        this.matY = mr.getMatchingPoint().getY();
        this.edgeId = mr.getEdgeWithInfo().getId();
        this.ratioToNextNode = mr.getRatioToNextNode();
        this.routeStart = mr.isRouteStart();
        this.timestamp = mr.getGpsPoint().getTimestamp();
        this.speed = mr.getGpsPoint().getSpeed();
    }

    public MatPoint(SerializedData.MatPointSer ser) {
        this.id = ser.getId();
        this.taxiId = ser.getTaxiId();
        this.oriX = ser.getOriX();
        this.oriY = ser.getOriY();
        this.matX = ser.getMatX();
        this.matY = ser.getMatY();
        this.edgeId = ser.getEdgeId();
        this.ratioToNextNode = ser.getRatioToNextNode();
        this.routeStart = ser.getRouteStart();
        this.timestamp = ser.getTimestamp();
        this.speed = ser.getSpeed();
    }

    public SerializedData.MatPointSer toSer() {
        return SerializedData.MatPointSer.newBuilder().setId(id).setTaxiId(taxiId).setOriX(oriX).setOriY(oriY).setMatX(matX).setMatY(matY)
                .setEdgeId(edgeId).setRatioToNextNode(ratioToNextNode).setRouteStart(routeStart).setTimestamp(timestamp)
                .setSpeed(speed).build();
    }

    @Override
    public Point getOriginalPoint() {
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
        Coordinate coordinate = new Coordinate(oriX, oriY);
        return geometryFactory.createPoint(coordinate);
    }

    @Override
    public Point getMatchingPoint() {
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
        Coordinate coordinate = new Coordinate(matX, matY);
        return geometryFactory.createPoint(coordinate);
    }

    @Override
    public Edge getEdge() {
        if (edge == null) {
            synchronized (this) {
                if (edge == null) {
                    try {
                        this.edge = QueryUtil.acquireEdgeById(edgeId);
                    } catch (SQLException | ParseException e) {
                        log.error("EDGE ID {} DO NOT EXIST.", edgeId);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return edge;
    }

    @Override
    public void update() {

    }
}
