package cn.edu.zju.gis.td.example.experiment.real;

import cn.edu.zju.gis.td.example.experiment.entity.RealAccumulator;
import cn.edu.zju.gis.td.example.experiment.entity.RealTimeStopInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author SUN Katus
 * @version 1.0, 2023-02-09
 */
@Slf4j
public class AverageInfoAggregate implements AggregateFunction<RealTimeStopInfo, RealAccumulator, RealAccumulator> {
    @Override
    public RealAccumulator createAccumulator() {
        return new RealAccumulator();
    }

    @Override
    public RealAccumulator add(RealTimeStopInfo value, RealAccumulator accumulator) {
        return accumulator.accumulate(value);
    }

    @Override
    public RealAccumulator getResult(RealAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public RealAccumulator merge(RealAccumulator a, RealAccumulator b) {
        return a.accumulate(b);
    }
}
