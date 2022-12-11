package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-08
 */
public class ClosestPathMatching extends RichMapFunction<GpsPoint, MatchingResult> implements Matching<GpsPoint, MatchingResult> {
    private transient ValueState<MatchingResult> lastMatchingResultState;

    @Override
    public boolean isCompatible(GpsPoint gpsPoint) throws IOException {
        MatchingResult previousMR = lastMatchingResultState.value();
        if (previousMR == null) {
            return false;
        }
        // 仅限新GPS时间与上一个时间不超过最大时间间隔
        return gpsPoint.getTimestamp() - previousMR.getGpsPoint().getTimestamp() < MatchingConstants.MAX_DELTA_TIME;
    }

    @Override
    public String name() {
        return "closest-path-matching";
    }

    @Override
    public MatchingResult map(GpsPoint gpsPoint) throws Exception {
        MatchingResult mr = null;
        if (!isCompatible(gpsPoint)) {
            mr = new ClosestDirectionAccurateMatching().map(gpsPoint);
            lastMatchingResultState.update(mr);
            return mr;
        }
        MatchingResult previousMR = lastMatchingResultState.value();
        // 丢弃重复的GPS数据
        if (previousMR.getGpsPoint().usefulValueEquals(gpsPoint)) {
            return null;
        }
        // 计算与上一次匹配点的间隔时间
        long deltaTime = gpsPoint.getTimestamp() - previousMR.getGpsPoint().getTimestamp();
        // 计算时间间隔内的最大可能通行范围
        double radius = MatchingConstants.MAX_ALLOW_SPEED * (deltaTime / 1000.0) + 2 * MatchingConstants.GPS_TOLERANCE;
        // 获取可能的最近匹配点
        List<MatchingResult> candidates = MatchingSQL.queryNearCandidates(gpsPoint);
        // 获取范围内的所有边ID
        Set<Long> edgeIds = MatchingSQL.queryEdgeIdsWithinRange(previousMR.getMatchingPoint(), radius);
        // 获取范围内的所有节点ID
        Map<Long, GraphNode> nodeGraphMap = MatchingSQL.queryNodeIdsWithinRange(previousMR.getMatchingPoint(), radius);
        // 起点是上一个匹配点所在边的终点
        Edge previousEdge = previousMR.getEdgeWithInfo();
        long lastStartId = previousEdge.getStartId();
        long oriStartId = previousEdge.getEndId(), startId = oriStartId;
        // 初始化数据结构
        for (Map.Entry<Long, GraphNode> entry : nodeGraphMap.entrySet()) {
            entry.setValue(new GraphNode(entry.getKey()));
        }
        // 设置起点信息
        GraphNode startGraphNode = nodeGraphMap.get(startId);
        startGraphNode.setCumulativeCost(previousMR.getRatioToNextNode() * previousEdge.cost());
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
            double minCost = 1.0 * Integer.MAX_VALUE;
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
            // 初始化直接与起点相连的信息
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
        // 判断候选点
        double minCost = 1.0 * Integer.MAX_VALUE;
        for (MatchingResult candidate : candidates) {
            candidate.update();
            EdgeWithInfo edgeWithInfo = candidate.getEdgeWithInfo();
            if (edgeWithInfo.getId() == previousEdge.getId()) {
                mr = candidate;
                break;
            }
            double cost = nodeGraphMap.get(edgeWithInfo.getStartId()).getCumulativeCost() + candidate.getRatioToNextNode() * edgeWithInfo.cost();
            if (cost < minCost) {
                minCost = cost;
                mr = candidate;
            }
        }
        return mr;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.lastMatchingResultState = getRuntimeContext().getState(new ValueStateDescriptor<>("previous-matching-result", MatchingResult.class));
    }
}
