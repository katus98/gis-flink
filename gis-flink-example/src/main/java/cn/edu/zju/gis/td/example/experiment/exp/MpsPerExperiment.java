package cn.edu.zju.gis.td.example.experiment.exp;

import cn.edu.zju.gis.td.example.experiment.entity.LocationType;
import cn.edu.zju.gis.td.example.experiment.entity.MatPoint;
import cn.edu.zju.gis.td.example.experiment.entity.RealTimeStopInfo;
import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import cn.edu.zju.gis.td.example.experiment.entity.schema.MatPointSerSchema;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.real.AverageInfoAggregate;
import cn.edu.zju.gis.td.example.experiment.real.UniformSpeedCalculator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Arrays;

/**
 * 实时道路交通量计算单元 - 性能测试
 *
 * @author SUN Katus
 * @version 1.0, 2023-02-10
 */
@Slf4j
public class MpsPerExperiment {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            log.error("Invalid Params: [{}]", Arrays.toString(args));
            throw new RuntimeException("Invalid Params");
        }
        // 位置类型指定
        LocationType type;
        if ("edge".equals(args[0])) {
            type = LocationType.EDGE;
        } else {
            type = LocationType.CENTER_POINT;
        }
        // flink上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // MPS数据源
        KafkaSource<SerializedData.MatPointSer> mpsSource = KafkaSource.<SerializedData.MatPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_MPS_TOPIC)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new MatPointSerSchema())
                .build();
        // 交通量计算
        env.fromSource(mpsSource, WatermarkStrategy.<SerializedData.MatPointSer>forBoundedOutOfOrderness(Duration.ofMinutes(5L)).withIdleness(Duration.ofMinutes(1L)), GlobalConfig.KAFKA_MPS_TOPIC)
                .map((MapFunction<SerializedData.MatPointSer, MatPoint>) MatPoint::new)
                .keyBy(MatPoint::getTaxiId)
                .flatMap(new UniformSpeedCalculator<>(type))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<RealTimeStopInfo>forBoundedOutOfOrderness(Duration.ofMinutes(5L))
                        .withTimestampAssigner((ele, t) -> ele.getTimestamp())
                        .withIdleness(Duration.ofMinutes(5L))
                )
                .keyBy(RealTimeStopInfo::getId)
                .window(TumblingEventTimeWindows.of(Time.minutes(180L)))
                .aggregate(new AverageInfoAggregate())
                .print();
        // 执行
        env.execute("mps-" + args[0] + "-per-exp");
    }
}
