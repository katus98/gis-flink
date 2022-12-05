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
}
