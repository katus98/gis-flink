package cn.edu.zju.gis.td.example.demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author SUN Katus
 * @version 1.0, 2022-11-02
 */
public class DemoWithKafka {

    public static void main(String[] args) throws Exception {
        // 准备流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置运行模式 : 流模式 批模式 自动模式
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 加载数据源 : 此处从Kafka加载
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("43.143.61.6:9092")
                .setTopics("test")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> resSource = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");
        // 数据输出
        resSource.print();
        // 执行程序 指定当前程序名称
        env.execute("flink-connect-kafka");
    }
}
