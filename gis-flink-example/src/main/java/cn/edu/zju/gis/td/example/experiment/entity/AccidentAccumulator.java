package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;
import lombok.ToString;

/**
 * 交通事故累计器
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-31
 */
@Getter
@ToString
public class AccidentAccumulator {
    private long unitId;
    private double deathIndexNumber;

    public AccidentAccumulator() {
        this.unitId = -1L;
        this.deathIndexNumber = 0.0;
    }

    public AccidentAccumulator accumulate(AccidentPoint point) {
        this.unitId = point.getUnitId();
        this.deathIndexNumber += point.getDeathIndexNumber();
        return this;
    }

    public AccidentAccumulator accumulate(AccidentAccumulator acc) {
        if (unitId == -1L) {
            this.unitId = acc.getUnitId();
        }
        this.deathIndexNumber += acc.getDeathIndexNumber();
        return this;
    }
}
