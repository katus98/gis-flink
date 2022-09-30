package cn.edu.zju.gis.td.example.demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 快速开始
 *
 * @author SUN Katus
 * @version 1.0, 2022-09-27
 */
public class Demo1 {
    public static void main(String[] args) throws Exception {
        // 准备流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置运行模式 : 流模式 批模式 自动模式
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 加载数据源 : 此处从内存加载
        DataStreamSource<String> eleSource = env.fromElements("java,scala,php,c++", "java,scala,php", "java,scala", "java");
        // 数据转换
        SingleOutputStreamOperator<String> resSource = eleSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(",");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        }).map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s.toUpperCase();
            }
        });
        // 数据输出
        resSource.print();
        // 执行程序 指定当前程序名称
        env.execute("flink-hello-world");
    }
}
