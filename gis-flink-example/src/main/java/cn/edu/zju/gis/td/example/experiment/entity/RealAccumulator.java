package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;
import lombok.ToString;

/**
 * @author SUN Katus
 * @version 1.0, 2023-02-09
 */
@Getter
@ToString
public class RealAccumulator {
    private long unitId;
    private long timestamp;
    private double flow;
    private double speed;
    private long count;

    public RealAccumulator() {
        this.unitId = -1L;
        this.timestamp = 0L;
        this.flow = 0.0;
        this.speed = 0.0;
        this.count = 0L;
    }

    public RealAccumulator accumulate(RealTimeStopInfo stopInfo) {
        this.unitId = stopInfo.getId();
        this.timestamp = Math.max(timestamp, stopInfo.getTimestamp());
        this.flow += stopInfo.getFlow();
        this.speed = (speed * count + stopInfo.getSpeed()) / (count + 1);
        this.count += 1;
        return this;
    }

    public RealAccumulator accumulate(RealAccumulator acc) {
        if (this.unitId == -1L) {
            this.unitId = acc.getUnitId();
        }
        this.timestamp = Math.max(timestamp, acc.getTimestamp());
        this.flow += acc.getFlow();
        this.speed = (speed * count + acc.getSpeed() * acc.getCount()) / (count + acc.getCount());
        this.count += acc.getCount();
        return this;
    }
}
