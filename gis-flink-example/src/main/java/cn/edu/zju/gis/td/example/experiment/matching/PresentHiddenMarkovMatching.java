package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.GraphNode;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.GraphCalculator;
import cn.edu.zju.gis.td.example.experiment.global.ModelConstants;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;

/**
 * 当前隐马尔可夫匹配算法 (当前全局最优)
 * 每个GPS点都会计算目前最优匹配点并输出到流(实时性强, 无误差积累, 但是存在匹配跳跃)
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-13
 */
@Slf4j
public class PresentHiddenMarkovMatching extends HiddenMarkovMatching {
    @Override
    public boolean isCompatible(GpsPoint gpsPoint) throws IOException {
        GpsPoint previousGPS = gpsPointState.value();
        if (previousGPS == null) {
            return false;
        }
        // 仅限新GPS时间与上一个时间不超过最大时间间隔
        return gpsPoint.getTimestamp() - previousGPS.getTimestamp() < ModelConstants.MAX_DELTA_TIME;
    }

    @Override
    public String name() {
        return "present-hidden-markov-matching";
    }

    @Override
    public MatchingResult map(GpsPoint gpsPoint) throws Exception {
        MatchingResult mr;

        // 获取可能的最近匹配点
        List<MatchingResult> candidates = QueryUtil.queryNearCandidates(gpsPoint);
        // 如果当前位置不存在匹配点
        if (candidates.isEmpty()) {
            candidatesState.update(Collections.emptyList());
            filterProbabilitiesState.update(new double[0]);
            gpsPointState.update(null);
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
                if (maxP < eps[i]) {
                    maxP = eps[i];
                    mr = candidate;
                }
            }
            // 更新状态
            candidatesState.update(candidates);
            filterProbabilitiesState.update(eps);
            gpsPointState.update(gpsPoint);
            return mr;
        }

        // 获取状态值
        // 前一个位置的候选点
        List<MatchingResult> previousCandidates = new ArrayList<>();
        for (MatchingResult matchingResult : candidatesState.get()) {
            previousCandidates.add(matchingResult);
        }
        // 前一个位置的过滤概率
        double[] previousFps = filterProbabilitiesState.value();
        // 前一个GPS点
        GpsPoint previousGPS = gpsPointState.value();

        // 计算与上一次匹配点的间隔时间
        long deltaTime = gpsPoint.getTimestamp() - previousGPS.getTimestamp();
        // 计算时间间隔内的最大可能通行范围
        double radius = ModelConstants.MAX_ALLOW_SPEED * (deltaTime / 1000.0) + 2 * ModelConstants.GPS_TOLERANCE;
        // 获取范围内的所有边ID
        Set<Long> edgeIds = QueryUtil.queryEdgeIdsWithinRange(previousCandidates.get(0).getOriginalPoint(), radius);
        // 获取范围内的所有节点ID
        Map<Long, GraphNode> nodeGraphMap = QueryUtil.queryNodeIdsWithinRange(previousCandidates.get(0).getOriginalPoint(), radius);

        // 构建图计算器
        GraphCalculator calculator = new GraphCalculator(nodeGraphMap, edgeIds);
        // 计算路径距离与直线距离差值矩阵
        double[][] dts = new double[candidates.size()][previousCandidates.size()];
        for (int i = 0; i < previousCandidates.size(); i++) {
            MatchingResult previousCandidate = previousCandidates.get(i);
            // 防止一个都没有
            nodeGraphMap.put(previousCandidate.getEdgeWithInfo().getEndId(), null);
            calculator.setStartPoint(previousCandidate);
            for (int j = 0; j < candidates.size(); j++) {
                MatchingResult candidate = candidates.get(j);
                dts[j][i] = Math.abs(calculator.computeStraightDistance(candidate) - calculator.computeCost(candidate));
            }
        }
        // 计算转移概率
        double[][] tps = new double[candidates.size()][];
        for (int i = 0; i < candidates.size(); i++) {
            tps[i] = computeTransitionProbabilities(dts[i]);
        }

        // 增量计算过滤概率
        double[] filterPs = new double[candidates.size()];
        boolean allZero = true;
        for (int i = 0; i < candidates.size(); i++) {
            MatchingResult candidate = candidates.get(i);
            double maxTFp = 0.0;
            for (int j = 0; j < previousCandidates.size(); j++) {
                if (maxTFp < tps[i][j] * previousFps[j]) {
                    maxTFp = tps[i][j] * previousFps[j];
                    // 由于当前最优不需要追溯路径所以可以省略记录上游节点, 节省开销
                    // candidate.setPreviousMR(previousCandidates.get(j));
                }
            }
            filterPs[i] = maxTFp * eps[i];
            if (filterPs[i] > 0.0) {
                allZero = false;
            }
        }
        // 如果过滤概率全为0, 将发射概率视作过滤概率
        if (allZero) {
            filterPs = eps;
        }
        // 获取过滤概率最高的候选点
        double maxP = 0.0;
        int validCount = 0;
        mr = candidates.get(0);
        for (int i = 0; i < candidates.size(); i++) {
            MatchingResult candidate = candidates.get(i);
            if (allZero) {
                candidate.setRouteStart(true);
            }
            if (filterPs[i] > 0) {
                validCount++;
            }
            if (filterPs[i] > maxP) {
                maxP = filterPs[i];
                mr = candidate;
            }
        }

        // 去除累积概率为0的候选点, 减轻后续运算压力
        previousFps = new double[validCount];
        int fi = 0;
        for (int i = 0; i < filterPs.length; i++) {
            if (filterPs[i] > 0) {
                // 将过滤概率等比扩大防止精度不足导致的损失
                previousFps[fi++] = filterPs[i] * (1 / maxP);
            } else {
                // 移除过滤概率为0的候选点, 减少后续运算压力
                candidates.remove(i - (filterPs.length - candidates.size()));
            }
        }
        // 更新变量状态
        filterProbabilitiesState.update(previousFps);
        candidatesState.update(candidates);
        gpsPointState.update(gpsPoint);
        return mr;
    }
}
