package cn.edu.zju.gis.td.example.experiment.global;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-05
 */
public final class GlobalConfig {
    /**
     * 数据库
     */
    public static final HikariDataSource PG_ORI_SOURCE;
    public static final HikariDataSource PG_GRAPH_SOURCE;
    public static final HikariDataSource PG_ANA_SOURCE;

    static {
        HikariConfig pgOriConfig = new HikariConfig(), pgGraphConfig = new HikariConfig(), pgAnaConfig = new HikariConfig();
        pgOriConfig.setJdbcUrl("jdbc:postgresql://10.79.231.85:5432/graduate_katus");
        pgOriConfig.setDriverClassName("org.postgresql.Driver");
        pgOriConfig.setUsername("postgres");
        pgOriConfig.setPassword("");
        pgGraphConfig.setJdbcUrl("jdbc:postgresql://10.79.231.84:5432/graduate_katus");
        pgGraphConfig.setDriverClassName("org.postgresql.Driver");
        pgGraphConfig.setUsername("postgres");
        pgGraphConfig.setPassword("");
        pgAnaConfig.setJdbcUrl("jdbc:postgresql://10.79.231.86:5432/graduate_katus");
        pgAnaConfig.setDriverClassName("org.postgresql.Driver");
        pgAnaConfig.setUsername("postgres");
        pgAnaConfig.setPassword("");
        PG_ORI_SOURCE = new HikariDataSource(pgOriConfig);
        PG_GRAPH_SOURCE = new HikariDataSource(pgGraphConfig);
        PG_ANA_SOURCE = new HikariDataSource(pgAnaConfig);
    }

    /**
     * 消息队列
     */
    public static final String KAFKA_SERVER;
    public static final String KAFKA_GPS_TEST_TOPIC;
    public static final String KAFKA_GPS_TOPIC;
    public static final String KAFKA_MPS_TEST_TOPIC;
    public static final String KAFKA_MPS_TOPIC;
    public static final String KAFKA_ILLEGALITY_TOPIC;
    public static final String KAFKA_ACCIDENT_TOPIC;
    public static final long TIME_0501;
    public static final long TIME_0504;

    static {
        KAFKA_SERVER = "*:9092";
        KAFKA_GPS_TEST_TOPIC = "taxi-ser-1119";
        KAFKA_GPS_TOPIC = "gps-all";
        KAFKA_MPS_TEST_TOPIC = "mps-ser-1119";
        KAFKA_MPS_TOPIC = "mps-all";
        KAFKA_ILLEGALITY_TOPIC = "illegality";
        KAFKA_ACCIDENT_TOPIC = "accident";
        TIME_0501 = 1651334400000L;
        TIME_0504 = 1651593600000L;
    }

    /**
     * 几何属性
     */
    public static final int SRID_WGS84, SRID_WGS84_UTM_50N;
    public static final CoordinateReferenceSystem CRS_WGS84, CRS_WGS84_UTM_50N;
    public static final MathTransform TRANSFORM_G2P, TRANSFORM_P2G;

    static {
        SRID_WGS84 = 4326;
        SRID_WGS84_UTM_50N = 32650;
        try {
            CRS_WGS84 = CRS.decode(String.format("EPSG:%d", SRID_WGS84));
            CRS_WGS84_UTM_50N = CRS.decode(String.format("EPSG:%d", SRID_WGS84_UTM_50N));
            TRANSFORM_G2P = CRS.findMathTransform(CRS_WGS84, CRS_WGS84_UTM_50N, true);
            TRANSFORM_P2G = CRS.findMathTransform(CRS_WGS84_UTM_50N, CRS_WGS84, true);
        } catch (FactoryException e) {
            throw new RuntimeException(e);
        }
    }
}
