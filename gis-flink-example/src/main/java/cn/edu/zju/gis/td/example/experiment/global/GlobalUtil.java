package cn.edu.zju.gis.td.example.experiment.global;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.locationtech.jts.io.WKTReader;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-05
 */
public class GlobalUtil {
    public static ObjectMapper JSON_MAPPER;
    public static WKTReader WKT_READER;

    static {
        JSON_MAPPER = new ObjectMapper();
        WKT_READER = new WKTReader();
    }
}
