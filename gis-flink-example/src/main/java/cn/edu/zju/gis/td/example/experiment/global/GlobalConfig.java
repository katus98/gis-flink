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
        pgOriConfig.setJdbcUrl("jdbc:postgresql://td2:5432/graduate_katus");
        pgOriConfig.setDriverClassName("org.postgresql.Driver");
        pgOriConfig.setUsername("postgres");
        pgOriConfig.setPassword("");
        pgOriConfig.setAutoCommit(true);
        pgOriConfig.setIdleTimeout(60000L);
        pgOriConfig.setConnectionTimeout(60000L);
        pgOriConfig.setMaxLifetime(0);
        pgOriConfig.setMinimumIdle(10);
        pgOriConfig.setMaximumPoolSize(10);
        pgGraphConfig.setJdbcUrl("jdbc:postgresql://td1:5432/graduate_katus");
        pgGraphConfig.setDriverClassName("org.postgresql.Driver");
        pgGraphConfig.setUsername("postgres");
        pgGraphConfig.setPassword("");
        pgGraphConfig.setAutoCommit(true);
        pgGraphConfig.setIdleTimeout(60000L);
        pgGraphConfig.setConnectionTimeout(60000L);
        pgGraphConfig.setMaxLifetime(0);
        pgGraphConfig.setMinimumIdle(10);
        pgGraphConfig.setMaximumPoolSize(10);
        pgAnaConfig.setJdbcUrl("jdbc:postgresql://td3:5432/graduate_katus");
        pgAnaConfig.setDriverClassName("org.postgresql.Driver");
        pgAnaConfig.setUsername("postgres");
        pgAnaConfig.setPassword("");
        pgAnaConfig.setAutoCommit(true);
        pgAnaConfig.setIdleTimeout(60000L);
        pgAnaConfig.setConnectionTimeout(60000L);
        pgAnaConfig.setMaxLifetime(0);
        pgAnaConfig.setMinimumIdle(10);
        pgAnaConfig.setMaximumPoolSize(10);
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
    public static final int KAFKA_PARTITION_COUNT;
    public static final long TIME_0501;
    public static final long TIME_0504;

    static {
        KAFKA_SERVER = "*:9092";
        KAFKA_GPS_TEST_TOPIC = "taxi-ser-1119";
        KAFKA_GPS_TOPIC = "gps-all-3";
        KAFKA_MPS_TEST_TOPIC = "mps-ser-1119";
        KAFKA_MPS_TOPIC = "mps-all-3";
        KAFKA_ILLEGALITY_TOPIC = "illegality";
        KAFKA_ACCIDENT_TOPIC = "accident";
        KAFKA_PARTITION_COUNT = 4;
        TIME_0501 = 1651334400000L;
        TIME_0504 = 1651593600000L;
    }

    /**
     * 几何属性
     */
    public static final int SRID_WGS84, SRID_WGS84_UTM_50N;
    public static final String WKT_WGS84, WKT_WGS84_UTM_50N;
    public static final CoordinateReferenceSystem CRS_WGS84, CRS_WGS84_UTM_50N;
    public static final MathTransform TRANSFORM_G2P, TRANSFORM_P2G;

    static {
        SRID_WGS84 = 4326;
        SRID_WGS84_UTM_50N = 32650;
        WKT_WGS84 = "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AUTHORITY[\"EPSG\",\"4326\"]]";
        WKT_WGS84_UTM_50N = "PROJCS[\"WGS 84 / UTM zone 50N\",GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AUTHORITY[\"EPSG\",\"4326\"]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0],PARAMETER[\"central_meridian\",117],PARAMETER[\"scale_factor\",0.9996],PARAMETER[\"false_easting\",500000],PARAMETER[\"false_northing\",0],UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],AXIS[\"Easting\",EAST],AXIS[\"Northing\",NORTH],AUTHORITY[\"EPSG\",\"32650\"]]";
        try {
            CRS_WGS84 = CRS.parseWKT(WKT_WGS84);
            CRS_WGS84_UTM_50N = CRS.parseWKT(WKT_WGS84_UTM_50N);
            TRANSFORM_G2P = CRS.findMathTransform(CRS_WGS84, CRS_WGS84_UTM_50N, true);
            TRANSFORM_P2G = CRS.findMathTransform(CRS_WGS84_UTM_50N, CRS_WGS84, true);
        } catch (FactoryException e) {
            throw new RuntimeException(e);
        }
    }
}
