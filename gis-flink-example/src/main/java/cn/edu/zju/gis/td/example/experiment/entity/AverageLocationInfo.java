package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;

/**
 * 时间窗口内的位置信息
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-07
 */
@Getter
public class AverageLocationInfo {
    private final long id;
    private final long timestamp;
    private final double flow;
    private final double speed;

    public AverageLocationInfo(long id, long timestamp, double flow, double speed) {
        this.id = id;
        this.timestamp = timestamp;
        this.flow = flow;
        this.speed = speed;
    }

    public String toLine() {
        return id + "," + timestamp + "," + flow + "," + speed;
    }

    @Override
    public String toString() {
        return toLine();
    }
}
