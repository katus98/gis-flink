package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-30
 */
@Slf4j
@Getter
@Setter
@ToString
public class IllegalityPoint implements TrafficEvent {
    private long id;
    private long timestamp;
    private String address;
    private IllegalityType type;
    private double lon;
    private double lat;
    private long unitId;

    public IllegalityPoint(SerializedData.IllegalityPointSer ser) {
        this.id = ser.getId();
        this.timestamp = ser.getTimestamp();
        this.address = ser.getAddress();
        this.type = IllegalityType.parseFrom(ser.getType());
        this.lon = ser.getLon();
        this.lat = ser.getLat();
    }
}
