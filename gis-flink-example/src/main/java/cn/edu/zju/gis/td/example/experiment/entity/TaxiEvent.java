package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;

/**
 * 出租车事件
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-07
 */
@Getter
public class TaxiEvent {
    private final int taxiId;
    private final long timestamp;
    private final double speed;

    public TaxiEvent(int taxiId, long timestamp, double speed) {
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.speed = speed;
    }
}
