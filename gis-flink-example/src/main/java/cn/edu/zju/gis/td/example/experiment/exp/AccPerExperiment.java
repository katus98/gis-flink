package cn.edu.zju.gis.td.example.experiment.exp;

import cn.edu.zju.gis.td.example.experiment.entity.AccidentPoint;
import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import cn.edu.zju.gis.td.example.experiment.entity.schema.AccidentPointSerSchema;
import cn.edu.zju.gis.td.example.experiment.event.AccidentAggregate;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 实时道路交通事故汇集计算单元 - 性能测试
 *
 * @author SUN Katus
 * @version 1.0, 2023-02-08
 */
@Slf4j
public class AccPerExperiment {
    public static void main(String[] args) throws Exception {
        // flink上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 交通事故数据源
        KafkaSource<SerializedData.AccidentPointSer> accidentSource = KafkaSource.<SerializedData.AccidentPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_ACCIDENT_TOPIC)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AccidentPointSerSchema())
                .build();
        // 交通事故流更新数据库
        env.fromSource(accidentSource, WatermarkStrategy.<SerializedData.AccidentPointSer>forMonotonousTimestamps().withIdleness(Duration.ofMinutes(1L)), GlobalConfig.KAFKA_ACCIDENT_TOPIC)
                .map(ser -> {
                    AccidentPoint event = new AccidentPoint(ser);
                    QueryUtil.queryTrafficEventUnitId(event);
                    return event;
                })
                .keyBy((KeySelector<AccidentPoint, Long>) AccidentPoint::getUnitId)
                .window(TumblingEventTimeWindows.of(Time.days(365L)))
                .aggregate(new AccidentAggregate())
                .filter(acc -> acc.getUnitId() != -1L)
                .print();
        // 执行
        env.execute("acc-pre-exp");
    }
}
