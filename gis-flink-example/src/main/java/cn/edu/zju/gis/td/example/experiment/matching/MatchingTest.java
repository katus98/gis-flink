package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import cn.edu.zju.gis.td.example.experiment.entity.*;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.GlobalUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.locationtech.jts.io.ParseException;
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 地图匹配实验
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-07
 */
@Slf4j
public class MatchingTest {
    public static final List<Integer> BATCH_COUNT_LIST;

    static {
        BATCH_COUNT_LIST = new ArrayList<>();
    }

    public static void main(String[] args) throws Exception {
        GlobalUtil.initialize();
        matchingStream();
    }

    private static void matchingStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 设置数据源
        KafkaSource<SerializedData.GpsPointSer> source = KafkaSource.<SerializedData.GpsPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_GPS_TEST_TOPIC)
                .setStartingOffsets(OffsetsInitializer.timestamp(GlobalConfig.TIME_0501))
                .setValueOnlyDeserializer(new GpsPointSerSchema())
                .build();
        // 指定匹配算法类型
        Matching<GpsPoint, MatchingResult> matching = new CachedPresentHiddenMarkovMatching(5);
        // 是否输出到文件
        boolean outToFile = true;
        SingleOutputStreamOperator<MatchingResult> resultStream = env
                .fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), GlobalConfig.KAFKA_GPS_TEST_TOPIC)
                .map((MapFunction<SerializedData.GpsPointSer, GpsPoint>) GpsPoint::new)
                .keyBy((KeySelector<GpsPoint, Integer>) GpsPoint::getTaxiId)
                .flatMap(matching)
                .filter(Objects::nonNull);
        // 定向结果流输出
        if (outToFile) {
            resultStream.addSink(obtainFileSink(matching.name()));
        } else {
            resultStream.map(it -> new MatPoint(it).toSer()).sinkTo(obtainKafkaSink());
        }
        env.execute(matching.name());
    }

    private static SinkFunction<MatchingResult> obtainFileSink(String matchingName) throws IOException {
        // 设置输出路径
        String outFilename = "E:\\data\\graduation\\matching\\" + matchingName;
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        if (!fsManipulator.exists(outFilename)) {
            fsManipulator.makeDirectories(outFilename);
        }
        // 设置文件输出
        return StreamingFileSink
                .forRowFormat(new Path(outFilename), new SimpleStringEncoder<MatchingResult>())
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.of(10L, ChronoUnit.SECONDS))
                        .withInactivityInterval(Duration.of(30L, ChronoUnit.SECONDS))
                        .withMaxPartSize(new MemorySize(1024L * 1024L * 1024L))
                        .build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("mr-")
                        .withPartSuffix(".csv")
                        .build())
                .build();
    }

    private static Sink<SerializedData.MatPointSer> obtainKafkaSink() {
        return KafkaSink.<SerializedData.MatPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setRecordSerializer(KafkaRecordSerializationSchema.<SerializedData.MatPointSer>builder()
                        .setTopic(GlobalConfig.KAFKA_MPS_TEST_TOPIC)
                        .setKafkaValueSerializer(MatPointSerSchema.class)
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.NONE)
                .build();
    }

    private static void matchingGlobal() throws IOException, SQLException, TransformException, ParseException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        GlobalHiddenMarkovMatching globalHiddenMarkovMatching = new GlobalHiddenMarkovMatching();
        String[] filenames = fsManipulator.list("E:\\data\\graduation\\gpsFilter");
        String outPath = "E:\\data\\graduation\\matching\\" + globalHiddenMarkovMatching.name() + "\\";
        Set<String> outFilenameSet = Arrays.stream(fsManipulator.list(outPath)).collect(Collectors.toSet());
        for (String filename : filenames) {
            String name = filename.substring(filename.lastIndexOf("\\") + 1);
            if (outFilenameSet.contains(outPath + name)) {
                log.info("SKIP: {} ------", name);
                continue;
            }
            log.info("START: {} ------", name);
            LineIterator it = fsManipulator.getLineIterator(filename);
            List<GpsPoint> gpsList = new ArrayList<>();
            while (it.hasNext()) {
                gpsList.add(new GpsPoint(it.next()));
            }
            List<MatchingResult> resultList = globalHiddenMarkovMatching.match(gpsList);
            List<String> contents = new ArrayList<>();
            contents.add(MatchingResult.matchingTitle());
            for (MatchingResult matchingResult : resultList) {
                contents.add(matchingResult.toMatchingLine());
            }
            fsManipulator.writeTextToFile(outPath + name, contents);
            log.info("FINISH: {} ------", name);
        }
        log.info("ALL FINISHED!");
    }
}
