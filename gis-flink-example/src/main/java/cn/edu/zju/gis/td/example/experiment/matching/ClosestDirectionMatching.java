package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 最近道路方向匹配算法
 * 计算GPS方向吻合的最近道路分割
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-07
 */
public class ClosestDirectionMatching implements Matching<GpsPoint, MatchingResult> {

    @Override
    public boolean isCompatible(GpsPoint gpsPoint) {
        return gpsPoint.getSpeed() != 0.0 && gpsPoint.getDirect() != 0.0;
    }

    @Override
    public String name() {
        return "closest-direction-matching";
    }

    @Override
    public MatchingResult map(GpsPoint gpsPoint) throws Exception {
        if (!isCompatible(gpsPoint)) {
            return new ClosestMatching().map(gpsPoint);
        }
        String sql = String.format("WITH ip AS (SELECT ST_Transform(ST_GeomFromText('POINT(%f %f)', 4326), 32650) AS p)\n" +
                "SELECT edges_pair_jinhua.*, ST_Distance(geom, ip.p) AS dis, ST_AsText(ST_Transform(ST_ClosestPoint(geom, ip.p), 4326)) as cp\n" +
                "FROM edges_pair_jinhua, ip\n" +
                "WHERE ST_Intersects(geom, ST_Buffer(ip.p, 20))\n" +
                "ORDER BY dis", gpsPoint.getLon(), gpsPoint.getLat());
        Connection conn = GlobalConfig.PG_DATA_SOURCE.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        List<MatchingResult> resList = new ArrayList<>();
        while (rs.next()) {
            resList.add(new MatchingResult(gpsPoint, rs));
        }
        rs.close();
        stmt.close();
        conn.close();
        for (MatchingResult matchingResult : resList) {
            if (judgeDirections(matchingResult)) {
                matchingResult.update();
                return matchingResult;
            }
        }
        return null;
    }

    protected boolean judgeDirections(MatchingResult matchingResult) {
        List<Double> directions = matchingResult.getEdgeWithInfo().getDirections();
        for (Double direction : directions) {
            if (judgeDirection(direction, matchingResult.getGpsPoint().getDirect())) {
                return true;
            }
        }
        return false;
    }

    protected boolean judgeDirection(double d1, double d2) {
        double tmp = Math.abs(d1 - d2);
        return tmp <= 10 || Math.abs(tmp - 360) <= 10;
    }
}
