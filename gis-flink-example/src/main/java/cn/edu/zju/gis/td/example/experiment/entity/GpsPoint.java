package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author SUN Katus
 * @version 1.0, 2022-11-30
 */
@Getter
@Setter
@ToString
public class GpsPoint {
    private static final SimpleDateFormat FORMAT1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"), FORMAT2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private long id;
    private int taxiId;
    private double lon, lat;
    private double speed, height, direct, mileage;
    private long timestamp;

    public GpsPoint() {
    }

    public GpsPoint(String line) throws ParseException {
        String[] items = line.split(",");
        this.id = Long.parseLong(items[0]);
        this.taxiId = Integer.parseInt(items[1]);
        this.lon = Double.parseDouble(items[2]);
        this.lat = Double.parseDouble(items[3]);
        this.speed = Double.parseDouble(items[4]);
        this.height = Double.parseDouble(items[5]);
        this.direct = Double.parseDouble(items[6]);
        this.mileage = Double.parseDouble(items[7]);
        try {
            this.timestamp = FORMAT1.parse(items[8]).getTime();
        } catch (ParseException e) {
            this.timestamp = FORMAT2.parse(items[8]).getTime();
        }
    }

    public boolean usefulValueEquals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GpsPoint gpsPoint = (GpsPoint) o;
        return taxiId == gpsPoint.taxiId && Double.compare(gpsPoint.lon, lon) == 0 && Double.compare(gpsPoint.lat, lat) == 0 && Double.compare(gpsPoint.speed, speed) == 0 && Double.compare(gpsPoint.direct, direct) == 0 && timestamp == gpsPoint.timestamp;
    }
}
