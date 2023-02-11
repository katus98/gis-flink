package cn.edu.zju.gis.td.example.experiment.event;

import cn.edu.zju.gis.td.example.experiment.entity.IllegalityAccumulator;
import cn.edu.zju.gis.td.example.experiment.entity.IllegalityPoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 交通违法统计方法
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-31
 */
@Slf4j
public class IllegalityAggregate implements AggregateFunction<IllegalityPoint, IllegalityAccumulator, IllegalityAccumulator> {
    @Override
    public IllegalityAccumulator createAccumulator() {
        return new IllegalityAccumulator();
    }

    @Override
    public IllegalityAccumulator add(IllegalityPoint illegalityPoint, IllegalityAccumulator illegalityAccumulator) {
        return illegalityAccumulator.accumulate(illegalityPoint);
    }

    @Override
    public IllegalityAccumulator getResult(IllegalityAccumulator illegalityAccumulator) {
        return illegalityAccumulator;
    }

    @Override
    public IllegalityAccumulator merge(IllegalityAccumulator illegalityAccumulator, IllegalityAccumulator acc1) {
        return illegalityAccumulator.accumulate(acc1);
    }
}
