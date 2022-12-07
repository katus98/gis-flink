package cn.edu.zju.gis.td.example.experiment.global;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-05
 */
public final class GlobalConfig {
    /**
     * 数据库
     */
    public static final HikariConfig PG_DATA_SOURCE_CONFIG;
    public static final HikariDataSource PG_DATA_SOURCE;

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
    public static final String KAFKA_SERVER;
    public static final String KAFKA_GPS_TOPIC;
    public static final String KAFKA_ILLEGALITY_TOPIC;
    public static final String KAFKA_ACCIDENT_TOPIC;
    public static final long TIME_0501;

    static {
        KAFKA_SERVER = "*:9092";
        KAFKA_GPS_TOPIC = "taxi-test-0501";
        KAFKA_ILLEGALITY_TOPIC = "illegal";
        KAFKA_ACCIDENT_TOPIC = "accident";
        TIME_0501 = 1651334400000L;
    }

    /**
     * 几何属性
     */
    public static final int SRID_WGS84 = 4326;
    public static final int SRID_WGS84_UTM_50N = 32650;
    public static final CoordinateReferenceSystem CRS_WGS84;
    public static final CoordinateReferenceSystem CRS_WGS84_UTM_50N;

    static {
        try {
            CRS_WGS84 = CRS.decode(String.format("EPSG:%d", SRID_WGS84));
            CRS_WGS84_UTM_50N = CRS.decode(String.format("EPSG:%d", SRID_WGS84_UTM_50N));
        } catch (FactoryException e) {
            throw new RuntimeException(e);
        }
    }
}
