package cn.edu.zju.gis.td.example.experiment.real;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.example.experiment.entity.*;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.GlobalUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * 实时数据计算实验
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-02
 */
@Slf4j
public class RealTimeInfoTest {
    public static void main(String[] args) throws Exception {
        GlobalUtil.initialize();
        realTimeDataCalculate();
    }

    private static void realTimeDataCalculate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 设置数据源
        KafkaSource<SerializedData.MatPointSer> source = KafkaSource.<SerializedData.MatPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_MPS_TOPIC)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new MatPointSerSchema())
                .build();
        // 创建匹配流并以taxiId作为key
        KeyedStream<MatPoint, Integer> taxiIdKeyedMatPointStream = env
                .fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), GlobalConfig.KAFKA_MPS_TOPIC)
                .map((MapFunction<SerializedData.MatPointSer, MatPoint>) MatPoint::new)
                .keyBy(MatPoint::getTaxiId);
        String testName = "real-time-cal-test1";
        // 设置输出路径
        String edgeFilename = "E:\\data\\graduation\\real\\edge\\" + testName;
        String cpFilename = "E:\\data\\graduation\\real\\cp\\" + testName;
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        if (!fsManipulator.exists(edgeFilename)) {
            fsManipulator.makeDirectories(edgeFilename);
        }
        if (!fsManipulator.exists(cpFilename)) {
            fsManipulator.makeDirectories(cpFilename);
        }
        // 设置文件输出
        DefaultRollingPolicy<AverageLocationInfo, String> rollingPolicy = DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.of(10L, ChronoUnit.SECONDS))
                .withInactivityInterval(Duration.of(30L, ChronoUnit.SECONDS))
                .withMaxPartSize(new MemorySize(1024L * 1024L * 1024L))
                .build();
        StreamingFileSink<AverageLocationInfo> edgeSink = StreamingFileSink
                .forRowFormat(new Path(edgeFilename), new SimpleStringEncoder<AverageLocationInfo>())
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .withRollingPolicy(rollingPolicy)
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("rc-edge-")
                        .withPartSuffix(".csv")
                        .build())
                .build();
        StreamingFileSink<AverageLocationInfo> cpSink = StreamingFileSink
                .forRowFormat(new Path(cpFilename), new SimpleStringEncoder<AverageLocationInfo>())
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .withRollingPolicy(rollingPolicy)
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("rc-cp-")
                        .withPartSuffix(".csv")
                        .build())
                .build();
        // 流计算过程
        taxiIdKeyedMatPointStream
                .flatMap(new UniformSpeedCalculator(LocationType.EDGE))
                .keyBy(RealTimeStopInfo::getId)
                .map(new AverageInfoCalculator(LocationType.EDGE))
                .addSink(edgeSink);
        taxiIdKeyedMatPointStream
                .flatMap(new UniformSpeedCalculator(LocationType.CENTER_POINT))
                .keyBy(RealTimeStopInfo::getId)
                .map(new AverageInfoCalculator(LocationType.CENTER_POINT))
                .addSink(cpSink);
        env.execute(testName);
    }
}
