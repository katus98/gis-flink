package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-30
 */
@Slf4j
@Getter
@Setter
public class AccidentPoint implements TrafficEvent {
    private long id;
    private long timestamp;
    private String address;
    private double deathIndexNumber;
    private double lon;
    private double lat;
    private long unitId;

    public AccidentPoint(SerializedData.AccidentPointSer ser) {
        this.id = ser.getId();
        this.timestamp = ser.getTimestamp();
        this.address = ser.getAddress();
        this.deathIndexNumber = ser.getDeathNumber() + ser.getDeathNumber7() + ser.getDeathLaterNumber() +
                ser.getMissingNumber() + 0.035 * (ser.getInjuredNumber() + ser.getInjuredNumber7());
        this.lon = ser.getLon();
        this.lat = ser.getLat();
    }
}
