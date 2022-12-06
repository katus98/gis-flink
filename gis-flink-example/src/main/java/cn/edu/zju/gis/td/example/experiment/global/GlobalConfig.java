package cn.edu.zju.gis.td.example.experiment.global;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-05
 */
public final class GlobalConfig {
    /**
     * 数据库
     */
    public static HikariConfig PG_DATA_SOURCE_CONFIG;
    public static HikariDataSource PG_DATA_SOURCE;

    static {
        PG_DATA_SOURCE_CONFIG = new HikariConfig();
        PG_DATA_SOURCE_CONFIG.setJdbcUrl("jdbc:postgresql://10.79.231.85:5432/graduate_katus");
        PG_DATA_SOURCE_CONFIG.setDriverClassName("org.postgresql.Driver");
        PG_DATA_SOURCE_CONFIG.setUsername("postgres");
//        config.setPassword("");
        PG_DATA_SOURCE = new HikariDataSource(PG_DATA_SOURCE_CONFIG);
    }

    /**
     * 消息队列
     */
    public static String KAFKA_SERVER;
    public static String KAFKA_GPS_TOPIC;
    public static String KAFKA_ILLEGALITY_TOPIC;
    public static String KAFKA_ACCIDENT_TOPIC;
    public static long TIME_0501;

    static {
//        KAFKA_SERVER = "*:9092";
        KAFKA_GPS_TOPIC = "taxi-test-0501";
        KAFKA_ILLEGALITY_TOPIC = "illegal";
        KAFKA_ACCIDENT_TOPIC = "accident";
        TIME_0501 = 1651334400000L;
    }
}
