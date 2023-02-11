package cn.edu.zju.gis.td.example.experiment.exp;

import cn.edu.zju.gis.td.example.experiment.entity.IllegalityPoint;
import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import cn.edu.zju.gis.td.example.experiment.entity.schema.IllegalityPointSerSchema;
import cn.edu.zju.gis.td.example.experiment.event.IllegalityAggregate;
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
import java.util.Arrays;

/**
 * 实时道路交通违法汇集计算单元
 *
 * @author SUN Katus
 * @version 1.0, 2023-02-08
 */
@Slf4j
public class IllExperiment {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            log.error("Invalid Params: [{}]", Arrays.toString(args));
            throw new RuntimeException("Invalid Params");
        }
        // 结算时间戳, 单位毫秒
        final long endTime = Long.parseLong(args[0]);
        // 交通违法回溯时间, 单位分钟
        final int illegalityInterval = Integer.parseInt(args[1]);
        // 开始时间戳, 单位毫秒
        final long illegalityStartTime = endTime - (long) illegalityInterval * 60 * 1000;
        log.info("ILL EXP: E[{}] I[{}]", endTime, illegalityInterval);
        final long buffer = (long) (0.1 * illegalityInterval * 60 * 1000);
        // flink上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 交通违法数据源
        KafkaSource<SerializedData.IllegalityPointSer> illegalitySource = KafkaSource.<SerializedData.IllegalityPointSer>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_ILLEGALITY_TOPIC)
                .setStartingOffsets(OffsetsInitializer.timestamp(illegalityStartTime))
                .setValueOnlyDeserializer(new IllegalityPointSerSchema())
                .build();
        // 交通违法流更新数据库
        env.fromSource(illegalitySource, WatermarkStrategy.<SerializedData.IllegalityPointSer>forMonotonousTimestamps().withIdleness(Duration.ofMinutes(1L)), GlobalConfig.KAFKA_ILLEGALITY_TOPIC)
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
        env.execute("ill-exp-" + (endTime / 1000));
    }
}
