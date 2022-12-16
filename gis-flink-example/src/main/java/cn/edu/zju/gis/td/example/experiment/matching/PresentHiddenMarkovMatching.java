package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.GraphNode;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.GraphCalculator;

import java.util.*;

/**
 * 当前隐马尔可夫匹配算法 (当前全局最优)
 * 每个GPS点都会计算目前最优匹配点并输出到流(实时性强, 无误差积累, 但是存在匹配跳跃)
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-13
 */
public class PresentHiddenMarkovMatching extends HiddenMarkovMatching {
    @Override
    public boolean isCompatible(GpsPoint gpsPoint) throws Exception {
        Iterable<MatchingResult> previousMRIts = candidatesState.get();
        GpsPoint previousGPS = null;
        for (MatchingResult previousMR : previousMRIts) {
            previousGPS = previousMR.getGpsPoint();
            break;
        }
        if (previousGPS == null) {
            return false;
        }
        // 仅限新GPS时间与上一个时间不超过最大时间间隔
        return gpsPoint.getTimestamp() - previousGPS.getTimestamp() < MatchingConstants.MAX_DELTA_TIME;
    }

    @Override
    public String name() {
        return "present-hidden-markov-matching";
    }

    @Override
    public MatchingResult map(GpsPoint gpsPoint) throws Exception {
        MatchingResult mr = null;
        // 获取可能的最近匹配点
        List<MatchingResult> candidates = MatchingSQL.queryNearCandidates(gpsPoint);
        // 如果当前位置不存在匹配点
        if (candidates.isEmpty()) {
            candidatesState.update(Collections.emptyList());
            filterProbabilitiesState.update(new double[0]);
            return null;
        }
        // 计算发射概率
        double[] errors = new double[candidates.size()];
        for (int i = 0; i < candidates.size(); i++) {
            errors[i] = candidates.get(i).getError();
        }
        double[] eqs = computeEmissionProbabilities(errors);
        // 如果是GPS起始点
        if (!isCompatible(gpsPoint)) {
            // 将发射概率视作过滤概率
            double maxP = 0;
            for (int i = 0; i < candidates.size(); i++) {
                if (maxP < eqs[i]) {
                    maxP = eqs[i];
                    mr = candidates.get(i);
                }
            }
            // 更新状态
            candidatesState.update(candidates);
            filterProbabilitiesState.update(eqs);
            return mr;
        }
        Iterable<MatchingResult> previousMRIts = candidatesState.get();
        List<MatchingResult> previousCandidates = new ArrayList<>();
        for (MatchingResult previousMR : previousMRIts) {
            previousCandidates.add(previousMR);
        }
        double[] fps = filterProbabilitiesState.value();
        GpsPoint previousGPS = previousCandidates.get(0).getGpsPoint();
        // 计算与上一次匹配点的间隔时间
        long deltaTime = gpsPoint.getTimestamp() - previousGPS.getTimestamp();
        // 计算时间间隔内的最大可能通行范围
        double radius = MatchingConstants.MAX_ALLOW_SPEED * (deltaTime / 1000.0) + 2 * MatchingConstants.GPS_TOLERANCE;
        // 获取范围内的所有边ID
        Set<Long> edgeIds = MatchingSQL.queryEdgeIdsWithinRange(previousCandidates.get(0).getOriginalPoint(), radius);
        // 获取范围内的所有节点ID
        Map<Long, GraphNode> nodeGraphMap = MatchingSQL.queryNodeIdsWithinRange(previousCandidates.get(0).getOriginalPoint(), radius);
        // 构建图计算器
        GraphCalculator calculator = new GraphCalculator(nodeGraphMap, edgeIds);
        // 计算路径距离与直线距离差值矩阵
        double[][] dts = new double[candidates.size()][previousCandidates.size()];
        for (int i = 0; i < previousCandidates.size(); i++) {
            MatchingResult previousCandidate = previousCandidates.get(i);
            calculator.setStartMR(previousCandidate);
            for (int j = 0; j < candidates.size(); j++) {
                MatchingResult candidate = candidates.get(i);
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
        for (int i = 0; i < candidates.size(); i++) {
            double maxTp = 0.0;
            for (int j = 0; j < previousCandidates.size(); j++) {
                if (maxTp < tps[i][j]) {
                    maxTp = tps[i][j];
                }
            }
            filterPs[i] = fps[i] * maxTp * eqs[i];
        }
        // 获取过滤概率最高的候选点
        double maxP = 0;
        for (int i = 0; i < candidates.size(); i++) {
            if (filterPs[i] > maxP) {
                maxP = filterPs[i];
                mr = candidates.get(i);
            }
        }
        // 更新变量状态
        filterProbabilitiesState.update(filterPs);
        candidatesState.update(candidates);
        return mr;
    }
}
