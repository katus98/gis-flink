package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.common.collection.Tuple;
import cn.edu.zju.gis.td.example.experiment.entity.Edge;
import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.GraphNode;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.opengis.referencing.operation.TransformException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-10
 */
public final class MatchingSQL {
    /**
     * 图结构缓存(懒加载)
     */
    private static final Map<Long, List<Edge>> GRAPH;
    /**
     * 既是中心点又是图节点的ID集合
     */
    private static final Set<Long> BOTH_ID_SET;

    static {
        GRAPH = new ConcurrentHashMap<>();
        BOTH_ID_SET = new HashSet<>();
    }

    /**
     * 获取候选匹配点
     */
    public static List<MatchingResult> queryNearCandidates(GpsPoint gpsPoint, int limit) throws SQLException, TransformException, ParseException {
        List<MatchingResult> matchingList = new ArrayList<>();
        String sql = String.format("WITH ip AS (SELECT ST_Transform(ST_GeomFromText('POINT(%f %f)', %d), %d) AS p)\n" +
                "SELECT edges_pair_jinhua.*, ST_Distance(geom, ip.p) AS dis, ST_AsText(ST_ClosestPoint(geom, ip.p)) as cp\n" +
                "FROM edges_pair_jinhua, ip\n" +
                "WHERE ST_Intersects(geom, ST_Buffer(ip.p, %d))\n" +
                "ORDER BY dis", gpsPoint.getLon(), gpsPoint.getLat(), GlobalConfig.SRID_WGS84, GlobalConfig.SRID_WGS84_UTM_50N, MatchingConstants.MATCHING_TOLERANCE);
        if (limit > 0) {
            sql = sql + " LIMIT " + limit;
        }
        Connection conn = GlobalConfig.PG_ORI_SOURCE.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            matchingList.add(new MatchingResult(gpsPoint, rs));
        }
        rs.close();
        stmt.close();
        conn.close();
        return matchingList;
    }

    public static List<MatchingResult> queryNearCandidates(GpsPoint gpsPoint) throws SQLException, TransformException, ParseException {
        return queryNearCandidates(gpsPoint, -1);
    }

    /**
     * 获取空间范围内的所有边ID
     */
    public static Set<Long> queryEdgeIdsWithinRange(Point previousPoint, double radius) throws SQLException {
        Set<Long> idSet = new HashSet<>();
        String sql = String.format("SELECT id FROM edges_pair_jinhua WHERE ST_Intersects(geom, ST_Buffer(ST_SetSRID(ST_Point(%f, %f), %d), %f))",
                previousPoint.getX(), previousPoint.getY(), GlobalConfig.SRID_WGS84_UTM_50N, radius);
        Connection conn = GlobalConfig.PG_GRAPH_SOURCE.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            idSet.add(rs.getLong("id"));
        }
        rs.close();
        stmt.close();
        conn.close();
        return idSet;
    }

    /**
     * 获取空间范围内的所有节点ID
     */
    public static Map<Long, GraphNode> queryNodeIdsWithinRange(Point previousPoint, double radius) throws SQLException {
        Map<Long, GraphNode> idSet = new HashMap<>();
        String sql = String.format("SELECT id FROM graph_nodes_jinhua WHERE ST_Intersects(geom, ST_Buffer(ST_SetSRID(ST_Point(%f, %f), %d), %f))",
                previousPoint.getX(), previousPoint.getY(), GlobalConfig.SRID_WGS84_UTM_50N, radius);
        Connection conn = GlobalConfig.PG_GRAPH_SOURCE.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            idSet.put(rs.getLong("id"), null);
        }
        rs.close();
        stmt.close();
        conn.close();
        return idSet;
    }

    /**
     * 获取指定ID节点为起点的所有边
     */
    public static List<Edge> acquireAllEdges(long startId) throws SQLException {
        if (!GRAPH.containsKey(startId)) {
            List<Edge> list = new ArrayList<>();
            String sql = String.format("SELECT id, start_id, end_id, length, time, velocity, flow FROM graph_edges_jinhua WHERE start_id = %d", startId);
            Connection conn = GlobalConfig.PG_GRAPH_SOURCE.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                list.add(new Edge(rs));
            }
            rs.close();
            stmt.close();
            conn.close();
            GRAPH.put(startId, list);
        }
        return GRAPH.get(startId);
    }

    public static void loadBothIds() throws SQLException {
        String sql = String.format("SELECT id FROM graph_nodes_jinhua WHERE is_node IS TRUE AND id <= %d", MatchingConstants.CENTER_POINT_NUMBER);
        Connection conn = GlobalConfig.PG_GRAPH_SOURCE.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            BOTH_ID_SET.add(rs.getLong("id"));
        }
        rs.close();
        stmt.close();
        conn.close();
    }

    public static boolean isRealNode(long id) {
        return id > MatchingConstants.CENTER_POINT_NUMBER || BOTH_ID_SET.contains(id);
    }
}
