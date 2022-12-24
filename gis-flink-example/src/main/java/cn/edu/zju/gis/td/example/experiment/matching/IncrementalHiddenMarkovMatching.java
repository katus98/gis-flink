package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.GraphNode;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.GraphCalculator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 增量隐马尔可夫匹配算法 (局部最优)
 * 在前一个匹配点作为确定匹配结果的基础上, 对每个GPS点都会计算最优概率匹配点并输出到流(实时性很强, 计算效率高, 无匹配跳跃, 但存在误差积累)
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-13
 */
@Slf4j
public class IncrementalHiddenMarkovMatching extends HiddenMarkovMatching {
    private transient ValueState<MatchingResult> matchingResultState;

    @Override
    public boolean isCompatible(GpsPoint gpsPoint) throws IOException {
        MatchingResult previousMR = matchingResultState.value();
        if (previousMR == null) {
            return false;
        }
        // 仅限新GPS时间与上一个时间不超过最大时间间隔
        return gpsPoint.getTimestamp() - previousMR.getGpsPoint().getTimestamp() < MatchingConstants.MAX_DELTA_TIME;
    }

    @Override
    public String name() {
        return "incremental-hidden-markov-matching";
    }

    @Override
    public MatchingResult map(GpsPoint gpsPoint) throws Exception {
        MatchingResult mr;

        // 获取可能的最近匹配点
        List<MatchingResult> candidates = MatchingSQL.queryNearCandidates(gpsPoint);
        // 如果当前位置不存在匹配点
        if (candidates.isEmpty()) {
            matchingResultState.update(null);
            log.debug("Id {} point have bean deleted!", gpsPoint.getId());
            return null;
        }

        // 计算发射概率
        double[] errors = new double[candidates.size()];
        for (int i = 0; i < candidates.size(); i++) {
            errors[i] = candidates.get(i).getError();
        }
        double[] eps = computeEmissionProbabilities(errors);

        // 如果当前位置是一条route的起点
        if (!isCompatible(gpsPoint)) {
            // 将发射概率视作过滤概率
            double maxP = 0.0;
            mr = candidates.get(0);
            for (int i = 0; i < candidates.size(); i++) {
                MatchingResult candidate = candidates.get(i);
                candidate.setRouteStart(true);
                if (eps[i] > maxP) {
                    maxP = eps[i];
                    mr = candidate;
                }
            }
            // 更新状态
            matchingResultState.update(mr);
            return mr;
        }

        // 获取状态值
        MatchingResult previousMR = matchingResultState.value();

        // 计算与上一次匹配点的间隔时间
        long deltaTime = gpsPoint.getTimestamp() - previousMR.getGpsPoint().getTimestamp();
        // 计算时间间隔内的最大可能通行范围
        double radius = MatchingConstants.MAX_ALLOW_SPEED * (deltaTime / 1000.0) + 2 * MatchingConstants.GPS_TOLERANCE;
        // 获取范围内的所有边ID
        Set<Long> edgeIds = MatchingSQL.queryEdgeIdsWithinRange(previousMR.getMatchingPoint(), radius);
        // 获取范围内的所有节点ID
        Map<Long, GraphNode> nodeGraphMap = MatchingSQL.queryNodeIdsWithinRange(previousMR.getMatchingPoint(), radius);

        // 构建图计算器
        GraphCalculator calculator = new GraphCalculator(nodeGraphMap, edgeIds);
        calculator.setStartMR(previousMR);
        // 防止一个都没有
        nodeGraphMap.put(previousMR.getEdgeWithInfo().getEndId(), null);
        // 计算转移概率
        double[] dts = new double[candidates.size()];
        for (int i = 0; i < candidates.size(); i++) {
            MatchingResult candidate = candidates.get(i);
            dts[i] = Math.abs(calculator.computeStraightDistance(candidate) - calculator.computeCost(candidate));
        }
        double[] tps = computeTransitionProbabilities(dts);

        // 计算总概率最高的候选点
        double maxP = 0.0;
        mr = candidates.get(0);
        for (int i = 0; i < candidates.size(); i++) {
            double p = eps[i] * tps[i];
            if (p > maxP) {
                maxP = p;
                mr = candidates.get(i);
            }
        }

        // 更新状态
        matchingResultState.update(mr);
        return mr;
    }

    @Override
    public void open(Configuration parameters) {
        this.matchingResultState = getRuntimeContext().getState(new ValueStateDescriptor<>("matching-result", MatchingResult.class));
    }
}
