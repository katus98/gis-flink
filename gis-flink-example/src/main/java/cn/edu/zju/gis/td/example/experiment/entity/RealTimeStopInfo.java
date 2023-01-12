package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;

/**
 * 实时途经信息
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-02
 */
@Getter
public class RealTimeStopInfo {
    private final long id;
    private final int taxiId;
    private final long timestamp;
    private int flow;
    private final double speed;

    public RealTimeStopInfo(long id, int taxiId, long timestamp, double speed) {
        this.id = id;
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.flow = 1;
        this.speed = speed;
    }

    public static RealTimeStopInfo generateCheckInfo(long id, long checkTime) {
        return new RealTimeStopInfo(id, -1, checkTime, 0);
    }

    public void delFlow() {
        this.flow = 0;
    }

    public String toLine() {
        return id + "," + taxiId + "," + timestamp + "," + speed;
    }

    @Override
    public String toString() {
        return toLine();
    }
}
