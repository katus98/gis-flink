package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author SUN Katus
 * @version 1.0, 2022-11-30
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GpsPoint {
    private static final SimpleDateFormat FORMAT1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"), FORMAT2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private long id;
    private int taxiId;
    private double lon, lat;
    private double speed, height, direct, mileage;
    private long timestamp;

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
}
