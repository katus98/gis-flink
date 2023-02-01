package cn.edu.zju.gis.td.example.experiment.event;

import cn.edu.zju.gis.td.example.experiment.entity.AccidentAccumulator;
import cn.edu.zju.gis.td.example.experiment.entity.AccidentPoint;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 交通事故统计方法
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-31
 */
public class AccidentAggregate implements AggregateFunction<AccidentPoint, AccidentAccumulator, AccidentAccumulator> {
    @Override
    public AccidentAccumulator createAccumulator() {
        return new AccidentAccumulator();
    }

    @Override
    public AccidentAccumulator add(AccidentPoint accidentPoint, AccidentAccumulator accidentAccumulator) {
        return accidentAccumulator.accumulate(accidentPoint);
    }

    @Override
    public AccidentAccumulator getResult(AccidentAccumulator accidentAccumulator) {
        return accidentAccumulator;
    }

    @Override
    public AccidentAccumulator merge(AccidentAccumulator accidentAccumulator, AccidentAccumulator acc1) {
        return accidentAccumulator.accumulate(acc1);
    }
}
