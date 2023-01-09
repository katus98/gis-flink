package cn.edu.zju.gis.td.example.experiment.global;

import cn.edu.zju.gis.td.example.experiment.entity.*;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.opengis.referencing.operation.TransformException;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-10
 */
public final class QueryUtil {
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
        List<MatchingResult> matchingList = new LinkedList<>();
        String sql = String.format("WITH ip AS (SELECT ST_Transform(ST_GeomFromText('POINT(%f %f)', %d), %d) AS p)\n" +
                "SELECT edges_f_pair.*, ST_Distance(geom, ip.p) AS dis, ST_AsText(ST_ClosestPoint(geom, ip.p)) as cp\n" +
                "FROM edges_f_pair, ip\n" +
                "WHERE ST_DWithin(geom, ip.p, %d)\n" +
                "ORDER BY dis", gpsPoint.getLon(), gpsPoint.getLat(), GlobalConfig.SRID_WGS84, GlobalConfig.SRID_WGS84_UTM_50N, ModelConstants.MATCHING_TOLERANCE);
        Map<String, Boolean> osmFilterMap = new HashMap<>();
        Connection conn = GlobalConfig.PG_ORI_SOURCE.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            MatchingResult mr = new MatchingResult(gpsPoint, rs);
            EdgeWithInfo edgeWithInfo = mr.getEdgeWithInfo();
            // 直接排除隧道类型的道路参与候选
            if (edgeWithInfo.isTunnel()) {
                continue;
            }
            // 同一个OSM ID表示的单行路段只允许占据一个匹配位, 双行道则仅允许两个
            String osmId = edgeWithInfo.getOsmId();
            if (osmFilterMap.containsKey(osmId)) {
                if (osmFilterMap.get(osmId)) {
                    osmFilterMap.put(osmId, false);
                    matchingList.add(mr);
                }
            } else {
                osmFilterMap.put(osmId, !edgeWithInfo.isOneway());
                matchingList.add(mr);
            }
            // 如果结果集大小达到限制, 则中断记录结果 (因为对结果进行筛选所以不能注入SQL语句)
            if (matchingList.size() >= limit) {
                break;
            }
        }
        rs.close();
        stmt.close();
        conn.close();
        return matchingList;
    }

    public static List<MatchingResult> queryNearCandidates(GpsPoint gpsPoint) throws SQLException, TransformException, ParseException {
        return queryNearCandidates(gpsPoint, Integer.MAX_VALUE);
    }

    /**
     * 获取空间范围内的所有边ID
     */
    public static Set<Long> queryEdgeIdsWithinRange(Point centerPoint, double radius) throws SQLException {
        return queryEdgeIdsWithinRange(centerPoint.getX(), centerPoint.getY(), radius);
    }

    public static Set<Long> queryEdgeIdsWithinRange(double x, double y, double radius) throws SQLException {
        Set<Long> idSet = new HashSet<>();
        String sql = String.format("SELECT id FROM graph_edges_jinhua WHERE ST_Intersects(geom, ST_Buffer(ST_SetSRID(ST_Point(%f, %f), %d), %f))",
                x, y, GlobalConfig.SRID_WGS84_UTM_50N, radius);
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
    public static Map<Long, GraphNode> queryNodeIdsWithinRange(Point centerPoint, double radius) throws SQLException {
        return queryNodeIdsWithinRange(centerPoint.getX(), centerPoint.getY(), radius);
    }

    public static Map<Long, GraphNode> queryNodeIdsWithinRange(double x, double y, double radius) throws SQLException {
        Map<Long, GraphNode> idMap = new LinkedHashMap<>();
        String sql = String.format("SELECT id FROM graph_nodes_jinhua WHERE ST_Intersects(geom, ST_Buffer(ST_SetSRID(ST_Point(%f, %f), %d), %f))",
                x, y, GlobalConfig.SRID_WGS84_UTM_50N, radius);
        Connection conn = GlobalConfig.PG_GRAPH_SOURCE.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            idMap.put(rs.getLong("id"), null);
        }
        rs.close();
        stmt.close();
        conn.close();
        return idMap;
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

    /**
     * 根据边ID获取边信息
     */
    public static Edge acquireEdgeById(long id) throws SQLException, ParseException {
        String sql = String.format("SELECT id, start_id, end_id, length, time, velocity, flow FROM graph_edges_jinhua WHERE id = %d", id);
        Connection conn = GlobalConfig.PG_GRAPH_SOURCE.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Edge edge = null;
        while (rs.next()) {
            edge = new Edge(rs);
        }
        rs.close();
        stmt.close();
        conn.close();
        return edge;
    }

    /**
     * 根据ID从边信息中查询初始速度
     */
    public static LocationTaxis initEdgeLocationById(long id) throws SQLException {
        String sql = String.format("SELECT id, init_velocity FROM graph_edges_jinhua WHERE id = %d", id);
        Connection conn = GlobalConfig.PG_GRAPH_SOURCE.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        LocationTaxis locationTaxis = null;
        while (rs.next()) {
            locationTaxis = new LocationTaxis(rs);
        }
        rs.close();
        stmt.close();
        conn.close();
        return locationTaxis;
    }

    /**
     * 根据ID从分析单元信息中查询初始速度
     */
    public static LocationTaxis initAnaUnitsLocationById(long id) throws SQLException {
        String sql = String.format("SELECT id, init_velocity FROM analysis_units WHERE id = %d", id);
        Connection conn = GlobalConfig.PG_ANA_SOURCE.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        LocationTaxis locationTaxis = null;
        while (rs.next()) {
            locationTaxis = new LocationTaxis(rs);
        }
        rs.close();
        stmt.close();
        conn.close();
        return locationTaxis;
    }

    /**
     * 更新图边实时信息
     */
    public static void updateInfoToEdge(long id, double flow, double speed) throws SQLException {
        String sql = "UPDATE graph_edges_jinhua SET flow = ?, velocity = ?, time = length / (? / 3.6) WHERE id = ?";
        Connection conn = GlobalConfig.PG_GRAPH_SOURCE.getConnection();
        PreparedStatement preStmt = conn.prepareStatement(sql);
        preStmt.setDouble(1, flow);
        preStmt.setDouble(2, speed);
        preStmt.setDouble(3, speed);
        preStmt.setLong(4, id);
        preStmt.executeUpdate();
        preStmt.close();
        conn.close();
    }

    /**
     * 更新分析单元实时信息
     */
    public static void updateInfoToAnaUnit(long id, double flow, double speed) throws SQLException {
        String sql = "UPDATE analysis_units SET flow = ?, velocity = ? WHERE id = ?";
        Connection conn = GlobalConfig.PG_ANA_SOURCE.getConnection();
        PreparedStatement preStmt = conn.prepareStatement(sql);
        preStmt.setDouble(1, flow);
        preStmt.setDouble(2, speed);
        preStmt.setLong(3, id);
        preStmt.close();
        conn.close();
    }

    static void loadBothIds() throws SQLException {
        String sql = "SELECT id FROM nodes_f WHERE is_node IS TRUE AND is_center IS TRUE";
        BOTH_ID_SET.clear();
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
        return id > ModelConstants.CENTER_POINT_NUMBER || BOTH_ID_SET.contains(id);
    }
}
