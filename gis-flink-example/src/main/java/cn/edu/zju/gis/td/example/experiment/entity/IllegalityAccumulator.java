package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * 交通违法累计器
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-31
 */
@Getter
@ToString
public class IllegalityAccumulator {
    private long unitId;
    private long timestamp;
    private final Map<IllegalityType, Double> countMap;

    public IllegalityAccumulator() {
        this.unitId = -1L;
        this.timestamp = 0L;
        this.countMap = new HashMap<>();
        for (IllegalityType type : IllegalityType.ALL_TYPES) {
            countMap.put(type, 0.0);
        }
    }

    public double getByType(IllegalityType type) {
        return countMap.get(type);
    }

    public IllegalityAccumulator accumulate(IllegalityPoint point) {
        this.unitId = point.getUnitId();
        this.timestamp = Math.max(timestamp, point.getTimestamp());
        IllegalityType type = point.getType();
        double count = this.countMap.get(type);
        this.countMap.put(type, count + 1);
        return this;
    }

    public IllegalityAccumulator accumulate(IllegalityAccumulator acc) {
        if (unitId == -1L) {
            this.unitId = acc.getUnitId();
        }
        this.timestamp = Math.max(timestamp, acc.getTimestamp());
        for (IllegalityType type : IllegalityType.ALL_TYPES) {
            this.countMap.put(type, countMap.get(type) + acc.getByType(type));
        }
        return this;
    }
}
