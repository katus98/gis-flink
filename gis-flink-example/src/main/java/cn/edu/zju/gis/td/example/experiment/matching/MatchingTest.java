package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.GlobalUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.locationtech.jts.io.ParseException;
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-07
 */
@Slf4j
public class MatchingTest {
    public static void main(String[] args) throws Exception {
        GlobalUtil.initialize();
        matchingGlobal();
    }

    private static void matchingStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_GPS_TOPIC)
                .setStartingOffsets(OffsetsInitializer.timestamp(GlobalConfig.TIME_0501))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> resSource = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), GlobalConfig.KAFKA_GPS_TOPIC);
        Matching<GpsPoint, MatchingResult> matching = new ClosestMatching();
        resSource.map((MapFunction<String, GpsPoint>) s -> GlobalUtil.JSON_MAPPER.readValue(s, GpsPoint.class))
                .keyBy((KeySelector<GpsPoint, Integer>) GpsPoint::getTaxiId)
                .filter(new GpsPointFilter())
                .flatMap(matching)
                .filter(Objects::nonNull)
                .map(MatchingResult::toMatchingLine)
                .print();
        env.execute(matching.name());
    }

    private static void matchingGlobal() throws IOException, SQLException, TransformException, ParseException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        String[] filenames = fsManipulator.list("F:\\data\\graduation\\gpsFilter");
        String outPath = "F:\\data\\graduation\\globalMR\\";
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
            GlobalHiddenMarkovMatching globalHiddenMarkovMatching = new GlobalHiddenMarkovMatching();
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
