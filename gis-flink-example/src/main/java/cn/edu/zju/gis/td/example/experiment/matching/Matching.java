package cn.edu.zju.gis.td.example.experiment.matching;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-07
 */
public interface Matching<T, O> extends MapFunction<T, O>, FlatMapFunction<T, O> {

    boolean isCompatible(T t) throws IOException;

    String name();

    @Override
    default void flatMap(T t, Collector<O> collector) throws Exception {
        collector.collect(map(t));
    }
}
