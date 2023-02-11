package cn.edu.zju.gis.td.example.experiment.exp;

import cn.edu.zju.gis.td.example.experiment.entity.LocationType;
import cn.edu.zju.gis.td.example.experiment.entity.MatPoint;
import cn.edu.zju.gis.td.example.experiment.entity.RealTimeStopInfo;
import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import cn.edu.zju.gis.td.example.experiment.entity.schema.MatPointSerSchema;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
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
 * 实时道路交通量计算单元
 *
 * @author SUN Katus
 * @version 1.0, 2023-02-10
 */
@Slf4j
public class MpsExperiment {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            log.error("Invalid Params: [{}]", Arrays.toString(args));
            throw new RuntimeException("Invalid Params");
        }
        // 结算时间戳, 单位毫秒
        final long endTime = Long.parseLong(args[0]);
        // MPS回溯时间, 单位分钟
        final int mpsInterval = Integer.parseInt(args[1]);
        // 位置类型指定
        LocationType type;
        if ("edge".equals(args[2])) {
            type = LocationType.EDGE;
        } else {
            type = LocationType.CENTER_POINT;
        }
        // 开始时间戳, 单位毫秒
        final long mpsStartTime = endTime - (long) mpsInterval * 60 * 1000;
        log.info("EXP: E[{}] I[{}]", endTime, mpsInterval);
        final long buffer = (long) (0.1 * mpsInterval * 60 * 1000);
        // flink上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // MPS数据源
        KafkaSource<SerializedData.MatPointSer> mpsSource = KafkaSource.<SerializedData.MatPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_MPS_TOPIC)
                .setStartingOffsets(OffsetsInitializer.timestamp(mpsStartTime))
                .setValueOnlyDeserializer(new MatPointSerSchema())
                .build();
        // 交通量计算
        env.fromSource(mpsSource, WatermarkStrategy.<SerializedData.MatPointSer>forBoundedOutOfOrderness(Duration.ofMinutes(5L)).withIdleness(Duration.ofMinutes(1L)), GlobalConfig.KAFKA_MPS_TOPIC)
                .filter(ser -> ser.getTimestamp() < endTime + buffer)
                .map((MapFunction<SerializedData.MatPointSer, MatPoint>) MatPoint::new)
                .keyBy(MatPoint::getTaxiId)
                .flatMap(new UniformSpeedCalculator<>(type))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<RealTimeStopInfo>forBoundedOutOfOrderness(Duration.ofMinutes(5L))
                        .withTimestampAssigner((ele, t) -> ele.getTimestamp())
                        .withIdleness(Duration.ofMinutes(5L))
                )
                .keyBy(RealTimeStopInfo::getId)
                .window(TumblingEventTimeWindows.of(Time.minutes(mpsInterval)))
                .aggregate(new AverageInfoAggregate())
                .filter(acc -> acc.getTimestamp() < endTime)
                .map(acc -> {
                    if (acc.getUnitId() != -1L) {
                        if (LocationType.EDGE.equals(type)) {
                            QueryUtil.updateInfoToEdge(acc.getUnitId(), acc.getFlow(), acc.getSpeed());
                        } else {
                            QueryUtil.updateInfoToAnaUnit(acc.getUnitId(), acc.getFlow(), acc.getSpeed());
                        }
                    }
                    return acc;
                })
                .print();
        // 执行
        env.execute("mps-" + args[2] + "-exp-" + (endTime / 1000));
    }
}
