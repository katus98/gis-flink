package cn.edu.zju.gis.td.example.experiment.global;

import cn.edu.zju.gis.td.example.experiment.entity.Edge;
import cn.edu.zju.gis.td.example.experiment.entity.EdgeWithInfo;
import cn.edu.zju.gis.td.example.experiment.entity.GraphNode;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.matching.MatchingConstants;
import cn.edu.zju.gis.td.example.experiment.matching.MatchingSQL;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 基于 Dijkstra 最短路径算法的图计算器
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-13
 */
@Slf4j
public class GraphCalculator {
    /**
     * 图节点计算数据结构
     */
    private final Map<Long, GraphNode> nodeGraphMap;
    /**
     * 范围内的边ID集合
     */
    private final Set<Long> edgeIds;
    /**
     * 起点匹配点
     */
    private MatchingResult startMR;
    /**
     * 图计算是否完成
     */
    private volatile boolean isFinished;

    public GraphCalculator(Map<Long, GraphNode> nodeGraphMap, Set<Long> edgeIds) {
        this.nodeGraphMap = nodeGraphMap;
        this.edgeIds = edgeIds;
        this.startMR = null;
        this.isFinished = false;
    }

    public MatchingResult getStartMR() {
        return startMR;
    }

    public long getStartNodeId() {
        return startMR.getEdgeWithInfo().getEndId();
    }

    public long getStartEdgeId() {
        return startMR.getEdgeWithInfo().getId();
    }

    public void setStartMR(MatchingResult startMR) {
        startMR.update();
        this.startMR = startMR;
        this.isFinished = false;
    }

    public double computeCost(MatchingResult endMR) throws SQLException {
        buildNodeGraphMap();
        endMR.update();
        EdgeWithInfo edgeWithInfo = endMR.getEdgeWithInfo();
        if (edgeWithInfo.getId() == getStartEdgeId()) {
            return fixCost((startMR.getRatioToNextNode() - endMR.getRatioToNextNode()) * edgeWithInfo.cost());
        }
        return nodeGraphMap.containsKey(edgeWithInfo.getStartId()) ? nodeGraphMap.get(edgeWithInfo.getStartId()).getCumulativeCost() + (1 - endMR.getRatioToNextNode()) * edgeWithInfo.cost() : MatchingConstants.MAX_COST;
    }

    public double computeStraightDistance(MatchingResult endMR) {
        return startMR.getMatchingPoint().distance(endMR.getMatchingPoint());
    }

    private void initNodeGraphMap() {
        for (Map.Entry<Long, GraphNode> entry : nodeGraphMap.entrySet()) {
            entry.setValue(new GraphNode(entry.getKey()));
        }
    }

    private void buildNodeGraphMap() throws SQLException {
        if (!isFinished) {
            synchronized (this) {
                if (!isFinished) {
                    if (startMR == null) {
                        log.error("HAVE NOT Assigned the START Point");
                        throw new RuntimeException();
                    }
                    // 初始化数据结构
                    initNodeGraphMap();
                    // 起点是上一个匹配点所在边的终点
                    Edge startEdge = startMR.getEdgeWithInfo();
                    long lastStartId = startEdge.getStartId();
                    long oriStartId = getStartNodeId(), startId = oriStartId;
                    // 设置起点信息
                    GraphNode startGraphNode = nodeGraphMap.get(startId);
                    startGraphNode.setCumulativeCost(startMR.getRatioToNextNode() * startEdge.cost());
                    startGraphNode.setVisited(true);
                    // 获取从起点开始的所有边
                    List<Edge> edges = MatchingSQL.acquireAllEdges(startId);
                    boolean isRealNode = MatchingSQL.isRealNode(startId);
                    // 初始化直接与起点相连的信息
                    for (Edge edge : edges) {
                        long endId = edge.getEndId();
                        // 仅处理范围内的边和节点
                        if (edgeIds.contains(edge.getId()) && nodeGraphMap.containsKey(endId)) {
                            GraphNode graphNode = nodeGraphMap.get(endId);
                            if (isRealNode || lastStartId != endId) {
                                graphNode.setCumulativeCost(edge.cost() + startGraphNode.getCumulativeCost());
                                graphNode.setPreviousNodeId(startId);
                            }
                        }
                    }
                    // Dijkstra 最短路径算法
                    int n = nodeGraphMap.size();
                    for (int i = 1; i < n; i++) {
                        double minCost = MatchingConstants.MAX_COST;
                        for (Map.Entry<Long, GraphNode> entry : nodeGraphMap.entrySet()) {
                            long nodeId = entry.getKey();
                            GraphNode graphNode = entry.getValue();
                            if (!graphNode.isVisited() && graphNode.getCumulativeCost() < minCost) {
                                minCost = graphNode.getCumulativeCost();
                                startId = nodeId;
                            }
                        }
                        nodeGraphMap.get(startId).setVisited(true);
                        lastStartId = nodeGraphMap.get(startId).getPreviousNodeId();
                        edges = MatchingSQL.acquireAllEdges(startId);
                        isRealNode = MatchingSQL.isRealNode(startId);
                        for (Edge edge : edges) {
                            long endId = edge.getEndId();
                            // 仅处理范围内的边和节点
                            if (edgeIds.contains(edge.getId()) && nodeGraphMap.containsKey(endId)) {
                                GraphNode graphNode = nodeGraphMap.get(endId);
                                if (isRealNode || lastStartId != endId) {
                                    double newCost = edge.cost() + nodeGraphMap.get(startId).getCumulativeCost();
                                    if (graphNode.getCumulativeCost() > newCost) {
                                        graphNode.setCumulativeCost(newCost);
                                        graphNode.setPreviousNodeId(startId);
                                    }
                                }
                            }
                        }
                    }
                    this.isFinished = true;
                }
            }
        }
    }

    private static double fixCost(double cost) {
        return Math.max(cost, 0.0);
    }
}
