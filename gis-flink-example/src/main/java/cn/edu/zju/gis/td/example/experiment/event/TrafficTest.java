package cn.edu.zju.gis.td.example.experiment.event;

import cn.edu.zju.gis.td.example.experiment.entity.AccidentPoint;
import cn.edu.zju.gis.td.example.experiment.entity.IllegalityPoint;
import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import cn.edu.zju.gis.td.example.experiment.entity.schema.AccidentPointSerSchema;
import cn.edu.zju.gis.td.example.experiment.entity.schema.IllegalityPointSerSchema;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.GlobalUtil;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-31
 */
public class TrafficTest {
    public static void main(String[] args) throws Exception {
        GlobalUtil.initialize();
        accidentInput();
        illegalityInput();
    }

    private static void accidentInput() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 设置数据源
        KafkaSource<SerializedData.AccidentPointSer> source = KafkaSource.<SerializedData.AccidentPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_ACCIDENT_TOPIC)
                .setStartingOffsets(OffsetsInitializer.timestamp(GlobalConfig.TIME_0501))
                .setValueOnlyDeserializer(new AccidentPointSerSchema())
                .build();
        // 更新数据库
        env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), GlobalConfig.KAFKA_ACCIDENT_TOPIC)
                .map(ser -> {
                    AccidentPoint event = new AccidentPoint(ser);
                    QueryUtil.queryTrafficEventUnitId(event);
                    return event;
                })
                .keyBy((KeySelector<AccidentPoint, Long>) AccidentPoint::getUnitId)
                .window(TumblingEventTimeWindows.of(Time.hours(3L)))
                .aggregate(new AccidentAggregate())
                .filter(it -> it.getUnitId() != -1L)
                .map(acc -> {
                    QueryUtil.updateAccidentInfoToAnaUnit(acc);
                    return acc;
                })
                .print();
        String testName = "accident-test";
        env.execute(testName);
    }

    private static void illegalityInput() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 设置数据源
        KafkaSource<SerializedData.IllegalityPointSer> source = KafkaSource.<SerializedData.IllegalityPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_ILLEGALITY_TOPIC)
                .setStartingOffsets(OffsetsInitializer.timestamp(GlobalConfig.TIME_0501))
                .setValueOnlyDeserializer(new IllegalityPointSerSchema())
                .build();
        // 更新数据库
        env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), GlobalConfig.KAFKA_ILLEGALITY_TOPIC)
                .map(ser -> {
                    IllegalityPoint event = new IllegalityPoint(ser);
                    QueryUtil.queryTrafficEventUnitId(event);
                    return event;
                })
                .keyBy((KeySelector<IllegalityPoint, Long>) IllegalityPoint::getUnitId)
                .window(TumblingEventTimeWindows.of(Time.hours(3L)))
                .aggregate(new IllegalityAggregate())
                .filter(it -> it.getUnitId() != -1L)
                .map(acc -> {
                    QueryUtil.updateIllegalityInfoToAnaUnit(acc);
                    return acc;
                })
                .print();
        String testName = "illegality-test";
        env.execute(testName);
    }
}
