package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.GlobalUtil;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 最近道路匹配算法
 * 计算GPS点最近的道路, 如果是双行道则通过点位偏向确定方向
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-05
 */
public class ClosestMatching implements Matching<GpsPoint, MatchingResult> {

    @Override
    public boolean isCompatible(GpsPoint gpsPoint) {
        return true;
    }

    @Override
    public String name() {
        return "closest-matching";
    }

    @Override
    public MatchingResult map(GpsPoint gpsPoint) throws Exception {
        if (!isCompatible(gpsPoint)) {
            return null;
        }
        String sql = String.format("WITH ip AS (SELECT ST_Transform(ST_GeomFromText('POINT(%f %f)', 4326), 32650) AS p)\n" +
                "SELECT edges_pair_jinhua.*, ST_Distance(geom, ip.p) AS dis, ST_AsText(ST_Transform(ST_ClosestPoint(geom, ip.p), 4326)) as cp\n" +
                "FROM edges_pair_jinhua, ip\n" +
                "WHERE ST_Intersects(geom, ST_Buffer(ip.p, 20))\n" +
                "ORDER BY dis\n" +
                "LIMIT 2", gpsPoint.getLon(), gpsPoint.getLat());
        Connection conn = GlobalConfig.PG_DATA_SOURCE.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        List<MatchingResult> resList = new ArrayList<>();
        while (rs.next()) {
            MatchingResult matchingResult = new MatchingResult(gpsPoint, rs);
            if (matchingResult.getEdgeWithInfo().isOneway()) {
                matchingResult.update();
                return matchingResult;
            }
            resList.add(matchingResult);
        }
        rs.close();
        stmt.close();
        conn.close();
        switch (resList.size()) {
            case 1:
                resList.get(0).update();
                return resList.get(0);
            case 2:
                if (isRightBias(resList.get(0))) {
                    resList.get(0).update();
                    return resList.get(0);
                } else {
                    resList.get(1).update();
                    return resList.get(1);
                }
            default:
                return null;
        }
    }

    private static boolean isRightBias(MatchingResult matchingResult) {
        matchingResult.update();
        double d1 = GlobalUtil.calDirection(matchingResult.getMatchingSegment());
        GeometryFactory factory = JTSFactoryFinder.getGeometryFactory();
        LineString line = factory.createLineString(new Coordinate[]{matchingResult.getMatchingPoint().getCoordinate(), matchingResult.getOriginalPoint().getCoordinate()});
        double d2 = GlobalUtil.calDirection(line);
        return (d2 - d1 >= 0 && d2 - d1 < 180) || (d2 + 360 - d1 >= 0 && d2 + 360 - d1 < 180);
    }
}
