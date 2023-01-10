package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.GraphNode;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.GraphCalculator;
import cn.edu.zju.gis.td.example.experiment.global.ModelConstants;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
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
    public boolean isCompatible(GpsPoint gpsPoint) {
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
        double[] previousFps = null;
        // 前一个GPS点
        GpsPoint previousGPS = null;
        // 当前的route终点集合
        List<MatchingResult> routeList = new ArrayList<>();
        // 统计匹配数量
        long count = 0L;
        for (GpsPoint gpsPoint : gpsPoints) {
            // 获取可能的候选点
            List<MatchingResult> candidates = QueryUtil.queryNearCandidates(gpsPoint);

            // 如果当前位置不存在匹配点, 短路
            if (candidates.isEmpty()) {
                if (finalMR != null) {
                    // 中断route, 重置状态变量
                    finalMR = null;
                    previousCandidates = null;
                    previousFps = null;
                    previousGPS = null;
                }
                log.debug("Index {} point have bean deleted!", count++);
                continue;
            }

            // 计算发射概率
            double[] errors = new double[candidates.size()];
            for (int i = 0; i < candidates.size(); i++) {
                errors[i] = candidates.get(i).getError();
            }
            double[] eps = computeEmissionProbabilities(errors);
            log.debug("Index: {}, EPS: {}", count, Arrays.toString(eps));

            if (finalMR == null || gpsPoint.getTimestamp() - previousGPS.getTimestamp() > ModelConstants.MAX_DELTA_TIME) {   // 如果当前位置是一条route的起点
                // 将发射概率视作过滤概率
                double maxP = 0.0;
                finalMR = candidates.get(0);
                for (int i = 0; i < candidates.size(); i++) {
                    MatchingResult candidate = candidates.get(i);
                    candidate.setRouteStart(true);
                    if (maxP < eps[i]) {
                        maxP = eps[i];
                        finalMR = candidate;
                    }
                }
                // 更新状态
                previousFps = eps;
            } else {
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
                    calculator.setStartPoint(previousCandidate);
                    for (int j = 0; j < candidates.size(); j++) {
                        MatchingResult candidate = candidates.get(j);
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
                boolean allZero = true;
                for (int i = 0; i < candidates.size(); i++) {
                    MatchingResult candidate = candidates.get(i);
                    double maxTFp = 0.0;
                    for (int j = 0; j < previousCandidates.size(); j++) {
                        if (maxTFp < tps[i][j] * previousFps[j]) {
                            maxTFp = tps[i][j] * previousFps[j];
                            candidate.setPreviousMR(previousCandidates.get(j));
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
                finalMR = candidates.get(0);
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
                        finalMR = candidate;
                    }
                }

                // 更新变量状态
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
            }
            log.debug("Index: {}, FPS: {}", count, Arrays.toString(previousFps));
            previousCandidates = candidates;
            previousGPS = gpsPoint;
            if (finalMR != null) {
                if (finalMR.isRouteStart()) {
                    routeList.add(finalMR);
                } else {
                    routeList.set(routeList.size() - 1, finalMR);
                }
            }
            log.debug("{}/{} points have finished!", ++count, gpsPoints.size());
        }
        List<MatchingResult> resList = new LinkedList<>();
        for (MatchingResult mr : routeList) {
            int index = resList.size();
            MatchingResult ptr = mr;
            while (ptr != null) {
                resList.add(index, ptr);
                ptr = ptr.getPreviousMR();
            }
        }
        return resList;
    }
}
