package cn.edu.zju.gis.td.example.experiment.global;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTReader;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-05
 */
public final class GlobalUtil {
    public static ObjectMapper JSON_MAPPER;
    public static WKTReader WKT_READER;

    static {
        JSON_MAPPER = new ObjectMapper();
        WKT_READER = new WKTReader();
    }

    public static double calDirection(LineString line) {
        Point startPoint = line.getStartPoint(), endPoint = line.getEndPoint();
        double deltaX = endPoint.getX() - startPoint.getX(), deltaY = endPoint.getY() - startPoint.getY();
        if (deltaX == 0.0) {
            return deltaY >= 0 ? 0.0 : 180.0;   // 如果位置没有变化则方向角默认值为0.0
        }
        if (deltaY == 0.0) {
            return deltaX > 0 ? 90.0 : 270.0;
        }
        double red = Math.atan(deltaX / deltaY);
        if (deltaX > 0) {
            return deltaY > 0 ? red2Deg(red) : red2Deg(red + Math.PI);
        } else {
            return deltaY > 0 ? red2Deg(red + 2 * Math.PI) : red2Deg(red + Math.PI);
        }
    }

    public static double red2Deg(double red) {
        return red / Math.PI * 180.0;
    }
}
