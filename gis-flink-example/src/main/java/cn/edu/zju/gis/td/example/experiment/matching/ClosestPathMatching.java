package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.*;
import cn.edu.zju.gis.td.example.experiment.global.GraphCalculator;
import cn.edu.zju.gis.td.example.experiment.global.ModelConstants;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 最短路径匹配算法
 * 选择候选点中与上个匹配点之间路径成本最低的
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-08
 */
public class ClosestPathMatching extends RichMapFunction<GpsPoint, MatchingResult> implements Matching<GpsPoint, MatchingResult> {
    private transient ValueState<MatchingResult> matchingResultState;

    @Override
    public boolean isCompatible(GpsPoint gpsPoint) throws IOException {
        MatchingResult previousMR = matchingResultState.value();
        if (previousMR == null) {
            return false;
        }
        // 仅限新GPS时间与上一个时间不超过最大时间间隔
        return gpsPoint.getTimestamp() - previousMR.getGpsPoint().getTimestamp() < ModelConstants.MAX_DELTA_TIME;
    }

    @Override
    public String name() {
        return "closest-path-matching";
    }

    @Override
    public MatchingResult map(GpsPoint gpsPoint) throws Exception {
        MatchingResult mr = null;
        // 如果当前位置是一条route的起点
        if (!isCompatible(gpsPoint)) {
            mr = new ClosestDirectionAccurateMatching().map(gpsPoint);
            matchingResultState.update(mr);
            return mr;
        }
        // 获取状态值
        MatchingResult previousMR = matchingResultState.value();
        // 计算与上一次匹配点的间隔时间
        long deltaTime = gpsPoint.getTimestamp() - previousMR.getGpsPoint().getTimestamp();
        // 计算时间间隔内的最大可能通行范围
        double radius = ModelConstants.MAX_ALLOW_SPEED * (deltaTime / 1000.0) + 2 * ModelConstants.GPS_TOLERANCE;
        // 获取可能的最近匹配点
        List<MatchingResult> candidates = QueryUtil.queryNearCandidates(gpsPoint);
        if (candidates.isEmpty()) {
            matchingResultState.update(null);
            return null;
        }
        // 获取范围内的所有边ID
        Set<Long> edgeIds = QueryUtil.queryEdgeIdsWithinRange(previousMR.getMatchingPoint(), radius);
        // 获取范围内的所有节点ID
        Map<Long, GraphNode> nodeGraphMap = QueryUtil.queryNodeIdsWithinRange(previousMR.getMatchingPoint(), radius);
        // 构建图计算器
        GraphCalculator calculator = new GraphCalculator(nodeGraphMap, edgeIds);
        calculator.setStartPoint(previousMR);
        // 判断候选点
        double minCost = ModelConstants.MAX_COST;
        for (MatchingResult candidate : candidates) {
            double cost = calculator.computeCost(candidate);
            if (cost < minCost) {
                minCost = cost;
                mr = candidate;
            }
        }
        // 如果没有最短路径匹配点则退化为精确方向匹配
        if (mr == null) {
            mr = new ClosestDirectionAccurateMatching().map(gpsPoint);
        }
        matchingResultState.update(mr);
        return mr;
    }

    @Override
    public void open(Configuration parameters) {
        this.matchingResultState = getRuntimeContext().getState(new ValueStateDescriptor<>("matching-result", MatchingResult.class));
    }
}
