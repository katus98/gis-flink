package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.GlobalUtil;
import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-05
 */
public class ClosestMatching {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 加载GPS数据流
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(GlobalConfig.KAFKA_SERVER)
                .setTopics(GlobalConfig.KAFKA_GPS_TOPIC)
                .setStartingOffsets(OffsetsInitializer.timestamp(GlobalConfig.TIME_0501))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> resSource = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), GlobalConfig.KAFKA_GPS_TOPIC);
        // 最近地图匹配
        resSource.map((MapFunction<String, GpsPoint>) s -> GlobalUtil.JSON_MAPPER.readValue(s, GpsPoint.class))
                .map((MapFunction<GpsPoint, String>) gpsPoint -> {
                    String sql = String.format("WITH ip AS (SELECT ST_Transform(ST_GeomFromText('POINT(%f %f)', 4326), 32650) AS p) " +
                            "SELECT ST_AsText(ST_Transform(ST_ClosestPoint(geom, ip.p), 4326)) as cp " +
                            "FROM edges_jinhua, ip " +
                            "WHERE ST_Intersects(geom, ST_Buffer(ip.p, 20)) " +
                            "ORDER BY ST_Distance(geom, ip.p) " +
                            "LIMIT 1", gpsPoint.getLon(), gpsPoint.getLat());
                    Connection conn = GlobalConfig.PG_DATA_SOURCE.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(sql);
                    String[] cords = null;
                    while (rs.next()) {
                        String wkt = (String) rs.getObject(1);
                        cords = wkt.substring(6, wkt.length() - 1).split(" ");
                    }
                    rs.close();
                    stmt.close();
                    conn.close();
                    if (cords != null) {
                        double x = Double.parseDouble(cords[0]);
                        double y = Double.parseDouble(cords[1]);
                        return String.format("gps: (%f, %f) | match: (%f, %f)", gpsPoint.getLon(), gpsPoint.getLat(), x, y);
                    }
                    return String.format("gps: (%f, %f) | match: failed!", gpsPoint.getLon(), gpsPoint.getLat());
                })
                .print();
        // 执行程序 指定当前程序名称
        env.execute("closest-matching");
    }
}
