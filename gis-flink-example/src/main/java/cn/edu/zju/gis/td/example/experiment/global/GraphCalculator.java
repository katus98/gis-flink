package cn.edu.zju.gis.td.example.experiment.global;

import cn.edu.zju.gis.td.example.experiment.entity.*;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.*;

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
    private Matchable startPoint;
    /**
     * 图计算是否完成
     */
    private volatile boolean isFinished;

    public GraphCalculator(Map<Long, GraphNode> nodeGraphMap, Set<Long> edgeIds) {
        this.nodeGraphMap = nodeGraphMap;
        this.edgeIds = edgeIds;
        this.startPoint = null;
        this.isFinished = false;
    }

    public Matchable getStartPoint() {
        return startPoint;
    }

    public long getStartNodeId() {
        return startPoint.getEdge().getEndId();
    }

    public long getStartEdgeId() {
        return startPoint.getEdge().getId();
    }

    public void setStartPoint(Matchable startPoint) {
        startPoint.update();
        this.startPoint = startPoint;
        this.isFinished = false;
        // 将起始位置的末端点加入图, 防止图结构完全缺失导致的空指针异常
        this.nodeGraphMap.put(startPoint.getEdge().getEndId(), null);
    }

    /**
     * 计算最短路径长度
     */
    public double computeCost(Matchable endPoint) throws SQLException {
        endPoint.update();
        buildNodeGraphMap();
        Edge edgeWithInfo = endPoint.getEdge();
        if (edgeWithInfo.getId() == getStartEdgeId()) {
            return fixCost((startPoint.getRatioToNextNode() - endPoint.getRatioToNextNode()) * edgeWithInfo.cost());
        }
        return nodeGraphMap.containsKey(edgeWithInfo.getStartId()) ? nodeGraphMap.get(edgeWithInfo.getStartId()).getCumulativeCost() + (1 - endPoint.getRatioToNextNode()) * edgeWithInfo.cost() : ModelConstants.MAX_COST;
    }

    /**
     * 计算直线距离长度
     */
    public double computeStraightDistance(Matchable endPoint) {
        return startPoint.getMatchingPoint().distance(endPoint.getMatchingPoint());
    }

    /**
     * 获取最短路径上的全部结点ID
     */
    public List<StopInfo> acquireRouteAllNodeStops(Matchable endPoint) throws SQLException {
        endPoint.update();
        buildNodeGraphMap();
        List<StopInfo> stops = new LinkedList<>();
        Edge edgeWithInfo = endPoint.getEdge();
        if (edgeWithInfo.getId() != getStartEdgeId() && canArrived(endPoint)) {
            long node = edgeWithInfo.getStartId();
            while (node != -1L) {
                stops.add(0, new StopInfo(node, nodeGraphMap.get(node).getCumulativeCost()));
                node = nodeGraphMap.get(node).getPreviousNodeId();
            }
        }
        return stops;
    }

    /**
     * 获取最短路径上的真实结点ID
     */
    public List<StopInfo> acquireRouteRealNodeStops(Matchable endPoint) throws SQLException {
        List<StopInfo> allNodeIds = acquireRouteAllNodeStops(endPoint);
        List<StopInfo> nodeIds = new ArrayList<>();
        for (StopInfo stop : allNodeIds) {
            if (QueryUtil.isRealNode(stop.getId())) {
                nodeIds.add(stop);
            }
        }
        return nodeIds;
    }

    /**
     * 获取最短路径上的路段中心点ID
     */
    public List<StopInfo> acquireRouteCenterPointStops(Matchable endPoint) throws SQLException {
        List<StopInfo> allNodeIds = acquireRouteAllNodeStops(endPoint);
        List<StopInfo> nodeIds = new ArrayList<>();
        for (StopInfo stop : allNodeIds) {
            if (!QueryUtil.isRealNode(stop.getId())) {
                nodeIds.add(stop);
            }
        }
        return nodeIds;
    }

    /**
     * 终点是否可以到达
     */
    public boolean canArrived(Matchable endPoint) throws SQLException {
        endPoint.update();
        buildNodeGraphMap();
        Edge edgeWithInfo = endPoint.getEdge();
        if (edgeWithInfo.getId() == getStartEdgeId()) return true;
        return nodeGraphMap.containsKey(edgeWithInfo.getStartId()) && nodeGraphMap.get(edgeWithInfo.getStartId()).isVisited();
    }

    /**
     * 获取最短路径上的全部边ID
     */
    public List<StopInfo> acquireRouteAllEdgeStops(Matchable endPoint) throws SQLException {
        endPoint.update();
        buildNodeGraphMap();
        List<StopInfo> stops = new LinkedList<>();
        if (!canArrived(endPoint)) {
            return stops;
        }
        Edge edgeWithInfo = endPoint.getEdge();
        if (edgeWithInfo.getId() != getStartEdgeId()) {
            long node = edgeWithInfo.getStartId();
            stops.add(new StopInfo(edgeWithInfo.getId(), computeCost(endPoint)));
            while (node != -1L) {
                stops.add(0, new StopInfo(nodeGraphMap.get(node).getPresentEdgeId(), nodeGraphMap.get(node).getCumulativeCost()));
                node = nodeGraphMap.get(node).getPreviousNodeId();
            }
        } else {
            stops.add(new StopInfo(edgeWithInfo.getId(), computeCost(endPoint)));
        }
        return stops;
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
                    if (startPoint == null) {
                        log.error("HAVE NOT Assigned the START Point");
                        throw new RuntimeException();
                    }
                    // 初始化数据结构
                    initNodeGraphMap();
                    // 起点是上一个匹配点所在边的终点
                    Edge startEdge = startPoint.getEdge();
                    long lastStartId = startEdge.getStartId();
                    long oriStartId = getStartNodeId(), startId = oriStartId;
                    // 设置起点信息
                    GraphNode startGraphNode = nodeGraphMap.get(startId);
                    startGraphNode.setCumulativeCost(startPoint.getRatioToNextNode() * startEdge.cost());
                    startGraphNode.setVisited(true);
                    startGraphNode.setPresentEdgeId(startEdge.getId());
                    // 获取从起点开始的所有边
                    List<Edge> edges = QueryUtil.acquireAllEdges(startId);
                    boolean isRealNode = QueryUtil.isRealNode(startId);
                    // 初始化直接与起点相连的信息
                    for (Edge edge : edges) {
                        long endId = edge.getEndId();
                        // 仅处理范围内的边和节点
                        if (edgeIds.contains(edge.getId()) && nodeGraphMap.containsKey(endId)) {
                            GraphNode graphNode = nodeGraphMap.get(endId);
                            if (isRealNode || lastStartId != endId) {
                                graphNode.setCumulativeCost(edge.cost() + startGraphNode.getCumulativeCost());
                                graphNode.setPreviousNodeId(startId);
                                graphNode.setPresentEdgeId(edge.getId());
                            }
                        }
                    }
                    // Dijkstra 最短路径算法
                    int n = nodeGraphMap.size();
                    for (int i = 1; i < n; i++) {
                        double minCost = ModelConstants.MAX_COST;
                        for (Map.Entry<Long, GraphNode> entry : nodeGraphMap.entrySet()) {
                            long nodeId = entry.getKey();
                            GraphNode graphNode = entry.getValue();
                            if (!graphNode.isVisited() && graphNode.getCumulativeCost() < minCost) {
                                minCost = graphNode.getCumulativeCost();
                                startId = nodeId;
                            }
                        }
                        // 短路, 如果未访问节点中已经不存在可到达的节点则直接中断计算
                        if (minCost == ModelConstants.MAX_COST) {
                            break;
                        }
                        nodeGraphMap.get(startId).setVisited(true);
                        lastStartId = nodeGraphMap.get(startId).getPreviousNodeId();
                        edges = QueryUtil.acquireAllEdges(startId);
                        isRealNode = QueryUtil.isRealNode(startId);
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
                                        graphNode.setPresentEdgeId(edge.getId());
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
