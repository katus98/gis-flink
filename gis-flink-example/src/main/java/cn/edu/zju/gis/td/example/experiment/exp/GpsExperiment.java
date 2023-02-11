package cn.edu.zju.gis.td.example.experiment.exp;

import cn.edu.zju.gis.td.example.experiment.data.MpsPartitioner;
import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.MatPoint;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import cn.edu.zju.gis.td.example.experiment.entity.schema.GpsPointSerSchema;
import cn.edu.zju.gis.td.example.experiment.entity.schema.MatPointSerSchema;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.matching.CachedPresentHiddenMarkovMatching;
import cn.edu.zju.gis.td.example.experiment.matching.Matching;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Objects;

/**
 * 地图匹配计算单元
 *
 * @author SUN Katus
 * @version 1.0, 2023-02-08
 */
@Slf4j
public class GpsExperiment {
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
        // MPS数据汇
        KafkaSink<SerializedData.MatPointSer> mpsSink = KafkaSink.<SerializedData.MatPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setRecordSerializer(KafkaRecordSerializationSchema.<SerializedData.MatPointSer>builder()
                        .setTopic(GlobalConfig.KAFKA_MPS_TOPIC)
                        .setValueSerializationSchema(new MatPointSerSchema())
                        .setPartitioner(new MpsPartitioner(GlobalConfig.KAFKA_PARTITION_COUNT))
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
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
                .sinkTo(mpsSink);
        // 执行
        env.execute("gps-exp");
    }
}
