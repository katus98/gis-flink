package cn.edu.zju.gis.td.example.experiment.exp;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.MatPoint;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import cn.edu.zju.gis.td.example.experiment.entity.schema.GpsPointSerSchema;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.matching.CachedPresentHiddenMarkovMatching;
import cn.edu.zju.gis.td.example.experiment.matching.Matching;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Objects;

/**
 * @author SUN Katus
 * @version 1.0, 2023-02-22
 */
public class GpsPerExperiment {
    public static void main(String[] args) throws Exception {
        // flink上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // GPS数据源
        KafkaSource<SerializedData.GpsPointSer> gpsSource = KafkaSource.<SerializedData.GpsPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_GPS_TOPIC)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new GpsPointSerSchema())
                .build();
        // 地图匹配算法
        Matching<GpsPoint, MatchingResult> matching = new CachedPresentHiddenMarkovMatching(5);
        // 地图匹配
        env.fromSource(gpsSource, WatermarkStrategy.<SerializedData.GpsPointSer>forMonotonousTimestamps().withIdleness(Duration.ofMinutes(1L)), GlobalConfig.KAFKA_GPS_TOPIC)
                .map((MapFunction<SerializedData.GpsPointSer, GpsPoint>) GpsPoint::new)
                .keyBy((KeySelector<GpsPoint, Integer>) GpsPoint::getTaxiId)
                .flatMap(matching)
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<MatchingResult>forBoundedOutOfOrderness(Duration.ofMinutes(5L))
                        .withTimestampAssigner((mr, t) -> mr.getTimestamp())
                        .withIdleness(Duration.ofMinutes(5L))
                )
                .map(mr -> new MatPoint(mr).toSer())
                .print();
        // 执行
        env.execute("gps-per-exp");
    }
}
