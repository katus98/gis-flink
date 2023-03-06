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
import cn.edu.zju.gis.td.example.experiment.real.AverageInfoProcess;
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
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

/**
 * 理论实验
 * * 包含全部的计算单元
 *
 * @author SUN Katus
 * @version 1.0, 2023-02-01
 */
@Slf4j
public class MainExperiment {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            log.error("Invalid Params: [{}]", Arrays.toString(args));
            throw new RuntimeException("Invalid Params");
        }
        // 开始时间戳, 单位毫秒, 用于对齐所有的数据流
        final long startTime = Long.parseLong(args[0]);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // GPS数据源
        KafkaSource<SerializedData.GpsPointSer> gpsSource = KafkaSource.<SerializedData.GpsPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_GPS_TOPIC)
                .setStartingOffsets(OffsetsInitializer.timestamp(startTime))
                .setValueOnlyDeserializer(new GpsPointSerSchema())
                .build();
        // 交通事故数据源
        KafkaSource<SerializedData.AccidentPointSer> accidentSource = KafkaSource.<SerializedData.AccidentPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_ACCIDENT_TOPIC)
                .setStartingOffsets(OffsetsInitializer.timestamp(startTime))
                .setValueOnlyDeserializer(new AccidentPointSerSchema())
                .build();
        // 交通违法数据源
        KafkaSource<SerializedData.IllegalityPointSer> illegalitySource = KafkaSource.<SerializedData.IllegalityPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_ILLEGALITY_TOPIC)
                .setStartingOffsets(OffsetsInitializer.timestamp(startTime))
                .setValueOnlyDeserializer(new IllegalityPointSerSchema())
                .build();
        // 地图匹配算法 缓存式HMM 5个数量级过滤 10个静止阈值
        Matching<GpsPoint, MatchingResult> matching = new CachedPresentHiddenMarkovMatching(5);
        // 地图匹配
        KeyedStream<MatchingResult, Integer> taxiIdKeyedMRStream = env
                .fromSource(gpsSource, WatermarkStrategy.<SerializedData.GpsPointSer>forMonotonousTimestamps().withIdleness(Duration.ofHours(1L)), GlobalConfig.KAFKA_GPS_TOPIC)
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
        // 边交通量计算 3小时10分钟滑动
        taxiIdKeyedMRStream
                .flatMap(new UniformSpeedCalculator<>(LocationType.EDGE))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<RealTimeStopInfo>forBoundedOutOfOrderness(Duration.ofMinutes(5L))
                        .withTimestampAssigner((ele, t) -> ele.getTimestamp())
                        .withIdleness(Duration.ofMinutes(5L))
                )
                .keyBy(RealTimeStopInfo::getId)
                .window(SlidingProcessingTimeWindows.of(Time.hours(3L), Time.minutes(10L)))
                .process(new AverageInfoProcess(LocationType.EDGE))
                .print();
        // 分析单元交通量计算 3小时10分钟滑动
        taxiIdKeyedMRStream
                .flatMap(new UniformSpeedCalculator<>(LocationType.CENTER_POINT))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<RealTimeStopInfo>forBoundedOutOfOrderness(Duration.ofMinutes(5L))
                        .withTimestampAssigner((ele, t) -> ele.getTimestamp())
                        .withIdleness(Duration.ofMinutes(5L))
                )
                .keyBy(RealTimeStopInfo::getId)
                .window(SlidingProcessingTimeWindows.of(Time.hours(3L), Time.minutes(10L)))
                .process(new AverageInfoProcess(LocationType.CENTER_POINT))
                .print();
        // 交通事故流更新数据库 3年1天滑动
        env.fromSource(accidentSource, WatermarkStrategy.forMonotonousTimestamps(), GlobalConfig.KAFKA_ACCIDENT_TOPIC)
                .map(ser -> {
                    AccidentPoint event = new AccidentPoint(ser);
                    QueryUtil.queryTrafficEventUnitId(event);
                    return event;
                })
                .keyBy((KeySelector<AccidentPoint, Long>) AccidentPoint::getUnitId)
                .window(SlidingProcessingTimeWindows.of(Time.days(3 * 365L), Time.days(1L)))
                .aggregate(new AccidentAggregate())
                .filter(it -> it.getUnitId() != -1L)
                .map(acc -> {
                    QueryUtil.updateAccidentInfoToAnaUnit(acc);
                    return acc;
                })
                .print();
        // 交通违法流更新数据库 3年1天滑动
        env.fromSource(illegalitySource, WatermarkStrategy.forMonotonousTimestamps(), GlobalConfig.KAFKA_ILLEGALITY_TOPIC)
                .map(ser -> {
                    IllegalityPoint event = new IllegalityPoint(ser);
                    QueryUtil.queryTrafficEventUnitId(event);
                    return event;
                })
                .keyBy((KeySelector<IllegalityPoint, Long>) IllegalityPoint::getUnitId)
                .window(SlidingProcessingTimeWindows.of(Time.days(3 * 365L), Time.days(1L)))
                .aggregate(new IllegalityAggregate())
                .filter(it -> it.getUnitId() != -1L)
                .map(acc -> {
                    QueryUtil.updateIllegalityInfoToAnaUnit(acc);
                    return acc;
                })
                .print();
        // 执行
        env.execute("main-exp");
    }
}