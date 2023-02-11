package cn.edu.zju.gis.td.example.experiment.exp;

import cn.edu.zju.gis.td.example.experiment.entity.*;
import cn.edu.zju.gis.td.example.experiment.entity.schema.AccidentPointSerSchema;
import cn.edu.zju.gis.td.example.experiment.entity.schema.GpsPointSerSchema;
import cn.edu.zju.gis.td.example.experiment.entity.schema.IllegalityPointSerSchema;
import cn.edu.zju.gis.td.example.experiment.event.AccidentAggregate;
import cn.edu.zju.gis.td.example.experiment.event.IllegalityAggregate;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
import cn.edu.zju.gis.td.example.experiment.matching.CachedPresentHiddenMarkovMatching;
import cn.edu.zju.gis.td.example.experiment.matching.Matching;
import cn.edu.zju.gis.td.example.experiment.real.AverageInfoAggregate;
import cn.edu.zju.gis.td.example.experiment.real.UniformSpeedCalculator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

/**
 * 总实际实验
 * * 包含全部的计算单元
 *
 * @author SUN Katus
 * @version 1.0, 2023-02-07
 */
@Slf4j
public class TestExperiment {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            log.error("Invalid Params: [{}]", Arrays.toString(args));
            throw new RuntimeException("Invalid Params");
        }
        // 结算时间戳, 单位毫秒
        final long endTime = Long.parseLong(args[0]);
        // GPS回溯时间, 单位分钟
        final int gpsInterval = Integer.parseInt(args[1]);
        // 交通事故回溯时间, 单位分钟
        final int accidentInterval = Integer.parseInt(args[2]);
        // 交通违法回溯时间, 单位分钟
        final int illegalityInterval = Integer.parseInt(args[3]);
        // 开始时间戳, 单位毫秒
        final long gpsStartTime = endTime - (long) gpsInterval * 60 * 1000;
        final long accidentStartTime = endTime - (long) accidentInterval * 60 * 1000;
        final long illegalityStartTime = endTime - (long) illegalityInterval * 60 * 1000;
        log.info("EXP: E[{}] I[{}, {}, {}]", endTime, gpsInterval, accidentInterval, illegalityInterval);
        // 最小间隔时间
        int minInterval = Math.min(gpsInterval, Math.min(accidentInterval, illegalityInterval));
        final long buffer = (long) (0.1 * minInterval * 60 * 1000);
        // flink上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // GPS数据源
        KafkaSource<SerializedData.GpsPointSer> gpsSource = KafkaSource.<SerializedData.GpsPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_GPS_TOPIC)
                .setStartingOffsets(OffsetsInitializer.timestamp(gpsStartTime))
                .setValueOnlyDeserializer(new GpsPointSerSchema())
                .build();
        // 交通事故数据源
        KafkaSource<SerializedData.AccidentPointSer> accidentSource = KafkaSource.<SerializedData.AccidentPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_ACCIDENT_TOPIC)
                .setStartingOffsets(OffsetsInitializer.timestamp(accidentStartTime))
                .setValueOnlyDeserializer(new AccidentPointSerSchema())
                .build();
        // 交通违法数据源
        KafkaSource<SerializedData.IllegalityPointSer> illegalitySource = KafkaSource.<SerializedData.IllegalityPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_ILLEGALITY_TOPIC)
                .setStartingOffsets(OffsetsInitializer.timestamp(illegalityStartTime))
                .setValueOnlyDeserializer(new IllegalityPointSerSchema())
                .build();
        // 地图匹配算法
        Matching<GpsPoint, MatchingResult> matching = new CachedPresentHiddenMarkovMatching(5);
        // 地图匹配
        KeyedStream<MatchingResult, Integer> taxiIdKeyedMRStream = env
                .fromSource(gpsSource, WatermarkStrategy.<SerializedData.GpsPointSer>forMonotonousTimestamps().withIdleness(Duration.ofSeconds(30L)), GlobalConfig.KAFKA_GPS_TOPIC)
                .filter(ser -> ser.getTimestamp() < endTime + buffer)
                .map((MapFunction<SerializedData.GpsPointSer, GpsPoint>) GpsPoint::new)
                .keyBy((KeySelector<GpsPoint, Integer>) GpsPoint::getTaxiId)
                .flatMap(matching)
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<MatchingResult>forBoundedOutOfOrderness(Duration.ofMinutes(5L))
                        .withTimestampAssigner((ele, t) -> ele.getTimestamp())
                        .withIdleness(Duration.ofMinutes(5L))
                )
                .keyBy(MatchingResult::getTaxiId);
        // 边交通量计算
        taxiIdKeyedMRStream
                .flatMap(new UniformSpeedCalculator<>(LocationType.EDGE))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<RealTimeStopInfo>forBoundedOutOfOrderness(Duration.ofMinutes(5L))
                        .withTimestampAssigner((ele, t) -> ele.getTimestamp())
                        .withIdleness(Duration.ofMinutes(5L))
                )
                .keyBy(RealTimeStopInfo::getId)
                .window(TumblingEventTimeWindows.of(Time.minutes(gpsInterval)))
                .aggregate(new AverageInfoAggregate())
                .filter(acc -> acc.getTimestamp() < endTime)
                .map(acc -> {
                    if (acc.getUnitId() != -1L) {
                        QueryUtil.updateInfoToEdge(acc.getUnitId(), acc.getFlow(), acc.getSpeed());
                    }
                    return acc;
                })
                .print();
        // 分析单元交通量计算
        taxiIdKeyedMRStream
                .flatMap(new UniformSpeedCalculator<>(LocationType.CENTER_POINT))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<RealTimeStopInfo>forBoundedOutOfOrderness(Duration.ofMinutes(5L))
                        .withTimestampAssigner((ele, t) -> ele.getTimestamp())
                        .withIdleness(Duration.ofMinutes(5L))
                )
                .keyBy(RealTimeStopInfo::getId)
                .window(TumblingEventTimeWindows.of(Time.minutes(gpsInterval)))
                .aggregate(new AverageInfoAggregate())
                .filter(acc -> acc.getTimestamp() < endTime)
                .map(acc -> {
                    if (acc.getUnitId() != -1L) {
                        QueryUtil.updateInfoToAnaUnit(acc.getUnitId(), acc.getFlow(), acc.getSpeed());
                    }
                    return acc;
                })
                .print();
        // 交通事故流更新数据库
        env.fromSource(accidentSource, WatermarkStrategy.<SerializedData.AccidentPointSer>forMonotonousTimestamps().withIdleness(Duration.ofSeconds(30L)), GlobalConfig.KAFKA_ACCIDENT_TOPIC)
                .filter(ser -> ser.getTimestamp() < endTime + buffer)
                .map(ser -> {
                    AccidentPoint event = new AccidentPoint(ser);
                    QueryUtil.queryTrafficEventUnitId(event);
                    return event;
                })
                .keyBy((KeySelector<AccidentPoint, Long>) AccidentPoint::getUnitId)
                .window(TumblingEventTimeWindows.of(Time.minutes(accidentInterval)))
                .aggregate(new AccidentAggregate())
                .filter(acc -> acc.getUnitId() != -1L)
                .filter(acc -> acc.getTimestamp() < endTime)
                .map(acc -> {
                    QueryUtil.updateAccidentInfoToAnaUnit(acc);
                    return acc;
                })
                .print();
        // 交通违法流更新数据库
        env.fromSource(illegalitySource, WatermarkStrategy.<SerializedData.IllegalityPointSer>forMonotonousTimestamps().withIdleness(Duration.ofSeconds(30L)), GlobalConfig.KAFKA_ILLEGALITY_TOPIC)
                .filter(ser -> ser.getTimestamp() < endTime + buffer)
                .map(ser -> {
                    IllegalityPoint event = new IllegalityPoint(ser);
                    QueryUtil.queryTrafficEventUnitId(event);
                    return event;
                })
                .keyBy((KeySelector<IllegalityPoint, Long>) IllegalityPoint::getUnitId)
                .window(TumblingEventTimeWindows.of(Time.minutes(illegalityInterval)))
                .aggregate(new IllegalityAggregate())
                .filter(acc -> acc.getUnitId() != -1L)
                .filter(acc -> acc.getTimestamp() < endTime)
                .map(acc -> {
                    QueryUtil.updateIllegalityInfoToAnaUnit(acc);
                    return acc;
                })
                .print();
        // 执行
        env.execute("test-exp-" + (endTime / 1000));
    }
}
