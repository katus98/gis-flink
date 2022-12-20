package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.GraphNode;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.GraphCalculator;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.io.ParseException;
import org.opengis.referencing.operation.TransformException;

import java.sql.SQLException;
import java.util.*;

/**
 * 全局隐马尔可夫匹配算法
 * 仅当GPS连续段结束后才会将全局最优匹配结果输出到流(实时性差, 准确性高)
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-13
 */
@Slf4j
public class GlobalHiddenMarkovMatching extends HiddenMarkovMatching {
    @Override
    public boolean isCompatible(GpsPoint gpsPoint) throws Exception {
        return true;
    }

    @Override
    public String name() {
        return "global-hidden-markov-matching";
    }

    @Override
    @Deprecated
    public MatchingResult map(GpsPoint gpsPoint) throws Exception {
        log.error("Global Hidden Markov Matching DO NOT Support Map Transformation.");
        return null;
    }

    public List<MatchingResult> match(List<GpsPoint> gpsPoints) throws SQLException, TransformException, ParseException {
        // 当前route的最后一个匹配点
        MatchingResult finalMR = null;
        // 前一个位置的候选点
        List<MatchingResult> previousCandidates = null;
        // 前一个位置的过滤概率
        double[] previousFqs = null;
        // 前一个GPS点
        GpsPoint previousGPS = null;
        // 当前的route终点集合
        List<MatchingResult> routeList = new ArrayList<>();
        for (GpsPoint gpsPoint : gpsPoints) {
            // 获取可能的最近匹配点
            List<MatchingResult> candidates = MatchingSQL.queryNearCandidates(gpsPoint);
            // 如果当前位置不存在匹配点
            if (candidates.isEmpty()) {
                if (finalMR != null) {
                    // 中断route, 重置状态变量
                    routeList.add(finalMR);
                    finalMR = null;
                    previousCandidates = null;
                    previousFqs = null;
                    previousGPS = null;
                }
                continue;
            }
            // 计算发射概率
            double[] errors = new double[candidates.size()];
            for (int i = 0; i < candidates.size(); i++) {
                errors[i] = candidates.get(i).getError();
            }
            double[] eqs = computeEmissionProbabilities(errors);
            if (finalMR == null || gpsPoint.getTimestamp() - previousGPS.getTimestamp() > MatchingConstants.MAX_DELTA_TIME) {   // 如果当前位置是一条route的起点
                // 将发射概率视作过滤概率
                double maxP = 0;
                for (int i = 0; i < candidates.size(); i++) {
                    if (maxP < eqs[i]) {
                        maxP = eqs[i];
                        finalMR = candidates.get(i);
                    }
                }
                // 更新状态
                previousFqs = eqs;
            } else {
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
                // 计算转移概率矩阵
                double[][] tps = new double[candidates.size()][];
                for (int i = 0; i < candidates.size(); i++) {
                    tps[i] = computeTransitionProbabilities(dts[i]);
                }
                // 增量计算过滤概率
                double[] filterPs = new double[candidates.size()];
                for (int i = 0; i < candidates.size(); i++) {
                    MatchingResult candidate = candidates.get(i);
                    double maxTp = 0.0;
                    for (int j = 0; j < previousCandidates.size(); j++) {
                        if (maxTp < tps[i][j]) {
                            maxTp = tps[i][j];
                            candidate.setPreviousMR(previousCandidates.get(j));
                        }
                    }
                    filterPs[i] = previousFqs[i] * maxTp * eqs[i];
                }
                // 获取过滤概率最高的候选点
                double maxP = 0;
                for (int i = 0; i < candidates.size(); i++) {
                    if (filterPs[i] > maxP) {
                        maxP = filterPs[i];
                        finalMR = candidates.get(i);
                    }
                }
                // 更新变量状态
                previousFqs = filterPs;
            }
            previousCandidates = candidates;
            previousGPS = gpsPoint;
        }
        if ((routeList.isEmpty() && finalMR != null) || routeList.get(routeList.size() - 1) != finalMR) {
            routeList.add(finalMR);
        }
        List<MatchingResult> resList = new LinkedList<>();
        for (MatchingResult mr : routeList) {
            int index = routeList.size();
            MatchingResult ptr = mr;
            while (ptr != null) {
                resList.add(index, ptr);
                ptr = ptr.getPreviousMR();
            }
        }
        return resList;
    }
}