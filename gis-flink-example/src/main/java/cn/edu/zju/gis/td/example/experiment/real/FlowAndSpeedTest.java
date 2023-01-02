package cn.edu.zju.gis.td.example.experiment.real;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.example.experiment.entity.*;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.GlobalUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-02
 */
public class FlowAndSpeedTest {
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
                .setStartingOffsets(OffsetsInitializer.timestamp(GlobalConfig.TIME_0501))
                .setValueOnlyDeserializer(new MatPointSerSchema())
                .build();
        DataStreamSource<SerializedData.MatPointSer> resSource = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), GlobalConfig.KAFKA_MPS_TOPIC);
        // 指定算法类型
        FlowAndSpeedCalculator flowAndSpeedCalculator = new FlowAndSpeedCalculator();
        String testName = "real-time-cal-test1";
        // 设置输出路径
        String outFilename = "F:\\data\\graduation\\real\\" + testName;
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        if (!fsManipulator.exists(outFilename)) {
            fsManipulator.makeDirectories(outFilename);
        }
        // 设置文件输出
        StreamingFileSink<FlowAndSpeed> sink = StreamingFileSink
                .forRowFormat(new Path(outFilename), new SimpleStringEncoder<FlowAndSpeed>())
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.of(10L, ChronoUnit.SECONDS))
                        .withInactivityInterval(Duration.of(30L, ChronoUnit.SECONDS))
                        .withMaxPartSize(new MemorySize(1024L * 1024L * 1024L))
                        .build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("rc-")
                        .withPartSuffix(".csv")
                        .build())
                .build();
        // 流式匹配算法
        resSource.map((MapFunction<SerializedData.MatPointSer, MatPoint>) MatPoint::new)
                .keyBy((KeySelector<MatPoint, Integer>) MatPoint::getTaxiId)
                .flatMap(flowAndSpeedCalculator)
                .filter(Objects::nonNull)
                .addSink(sink);
        env.execute(testName);
    }
}
