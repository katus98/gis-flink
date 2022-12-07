package cn.edu.zju.gis.td.example.experiment.matching;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-07
 */
public interface Matching<T, O> extends MapFunction<T, O>, MapPartitionFunction<T, O> {

    boolean isCompatible(T t);

    String name();

    @Override
    default void mapPartition(Iterable<T> iterable, Collector<O> collector) throws Exception {
        for (T t : iterable) {
            collector.collect(map(t));
        }
    }
}
