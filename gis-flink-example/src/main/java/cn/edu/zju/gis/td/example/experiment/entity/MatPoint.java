package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-24
 */
@Getter
@Setter
@ToString
public class MatPoint {
    private long id;
    private double oriX;
    private double oriY;
    private double matX;
    private double matY;
    private long edgeId;
    private double ratioToNextNode;
    private boolean routeStart;
    private long timestamp;
    private double speed;

    public MatPoint(MatchingResult mr) {
        this.id = mr.getGpsPoint().getId();
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
        return SerializedData.MatPointSer.newBuilder().setId(id).setOriX(oriX).setOriY(oriY).setMatX(matX).setMatY(matY)
                .setEdgeId(edgeId).setRatioToNextNode(ratioToNextNode).setRouteStart(routeStart).setTimestamp(timestamp)
                .setSpeed(speed).build();
    }
}
