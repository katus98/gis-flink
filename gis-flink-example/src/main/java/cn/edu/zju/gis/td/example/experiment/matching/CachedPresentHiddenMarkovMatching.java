package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.GraphNode;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.GraphCalculator;
import cn.edu.zju.gis.td.example.experiment.global.ModelConstants;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 缓存当前隐马尔可夫匹配算法
 * 在当前隐马尔可夫匹配算法的基础上通过缓存尽可能逼近全局最优路径(实时性稍弱可调节, 误差积累小, 无匹配跳跃)
 * * 额外优化: 通过缓存局部最优解决直至可以排除不可能候选点时将缓存结果写入结果流
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-13
 */
@Slf4j
public class CachedPresentHiddenMarkovMatching extends PresentHiddenMarkovMatching {
    private static final int MAX_STOP = 10;
    private transient ValueState<Integer> bestIndexState;
    private transient ValueState<Integer> stopCountState;
    private final int magnitudeLevel;
    private final double quotient;

    public CachedPresentHiddenMarkovMatching(int magnitudeLevel) {
        this.magnitudeLevel = magnitudeLevel;
        this.quotient = Math.pow(10, magnitudeLevel);
    }

    @Override
    public String name() {
        return "cached-present-hidden-markov-matching-x-" + magnitudeLevel;
    }

    @Override
    @Deprecated
    public MatchingResult map(GpsPoint gpsPoint) throws Exception {
        log.error("Cached Present Hidden Markov Matching DO NOT Support Map Transformation.");
        return null;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.bestIndexState = getRuntimeContext().getState(new ValueStateDescriptor<>("best-index", Integer.class));
        this.stopCountState = getRuntimeContext().getState(new ValueStateDescriptor<>("stop-count", Integer.class));
    }

    @Override
    public void flatMap(GpsPoint gpsPoint, Collector<MatchingResult> collector) throws Exception {
        // 实验测试用
//        if (gpsPoint.getId() == -1L) {
//            int total = 0, min = Integer.MAX_VALUE, max = -1;
//            for (int count : MatchingTest.BATCH_COUNT_LIST) {
//                if (count > max) {
//                    max = count;
//                }
//                if (count < min) {
//                    min = count;
//                }
//                total += count;
//            }
//            int size = MatchingTest.BATCH_COUNT_LIST.size();
//            double avg = 1.0 * total / size;
//            Collections.sort(MatchingTest.BATCH_COUNT_LIST);
//            log.info("Batch Push Size -- TOL: {}", total);
//            log.info("Batch Push Size -- AVG: {}, MAX: {}, MIN: {}", avg, max, min);
//            int mid = MatchingTest.BATCH_COUNT_LIST.get(size / 2);
//            int mid20 = MatchingTest.BATCH_COUNT_LIST.get((int) (0.8 * size));
//            int mid5 = MatchingTest.BATCH_COUNT_LIST.get((int) (0.95 * size));
//            int mid1 = MatchingTest.BATCH_COUNT_LIST.get((int) (0.99 * size));
//            int mid01 = MatchingTest.BATCH_COUNT_LIST.get((int) (0.999 * size));
//            log.info("Batch Push Size -- MID: {}, MID 20%H: {}, MID 5%H: {}, MID 1%H: {}, MID 0.1%H: {}", mid, mid20, mid5, mid1, mid01);
//            log.info("{},{},{},{},{},{},{},{},{},{}", magnitudeLevel, total, avg, max, min, mid, mid20, mid5, mid1, mid01);
//            log.info("ALL FINISHED!");
//        }
        MatchingResult mr;

        // 获取状态值 - 1
        // 前一个位置的候选点
        List<MatchingResult> previousCandidates = new ArrayList<>();
        for (MatchingResult matchingResult : candidatesState.get()) {
            previousCandidates.add(matchingResult);
        }
        // 前一个当前最优匹配点索引号
        int previousIndex = bestIndexState.value() == null ? -1 : bestIndexState.value();
        // 前一个GPS点
        GpsPoint previousGPS = gpsPointState.value();

        // 对于停车等待的情况进行具有一定缓冲的截断
        if (gpsPoint.posEquals(previousGPS)) {
            int stopCount = stopCountState.value() == null ? 1 : stopCountState.value() + 1;
            stopCountState.update(stopCount);
            if (stopCount >= MAX_STOP) {
                if (previousIndex >= 0) {
                    MatchingResult previousMR = previousCandidates.get(previousIndex);
                    pushIntoStream(collector, previousMR);
                    mr = new MatchingResult(previousMR);
                    mr.setGpsPoint(gpsPoint);
                    mr.setRouteStart(true);
                    mr.setPreviousMR(null);
                    mr.setInStream(false);
                    filterProbabilitiesState.update(new double[]{1.0});
                    candidatesState.update(Collections.singletonList(mr));
                    gpsPointState.update(gpsPoint);
                    bestIndexState.update(0);
                }
                return;
            }
        } else {
            stopCountState.update(0);
        }

        // 获取可能的最近匹配点
        List<MatchingResult> candidates = QueryUtil.queryNearCandidates(gpsPoint);
        // 如果当前位置不存在匹配点
        if (candidates.isEmpty()) {
            candidatesState.update(Collections.emptyList());
            filterProbabilitiesState.update(new double[0]);
            gpsPointState.update(null);
            bestIndexState.update(-1);
            log.debug("Id {} point have bean deleted!", gpsPoint.getId());
            // 道路中断将缓存结果加入流
            if (previousIndex >= 0) {
                pushIntoStream(collector, previousCandidates.get(previousIndex));
            }
            return;
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
                    bestIndexState.update(i);
                }
            }
            // 道路匹配结果唯一将缓存结果加入流
            if (candidates.size() == 1) {
                if (previousIndex >= 0) {
                    pushIntoStream(collector, previousCandidates.get(previousIndex));
                }
                mr.setInStream(true);
                collector.collect(mr);
            }
            // 更新状态
            candidatesState.update(candidates);
            filterProbabilitiesState.update(eps);
            gpsPointState.update(gpsPoint);
            return;
        }

        // 获取状态值 - 2
        // 前一个位置的过滤概率
        double[] previousFps = filterProbabilitiesState.value();

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
                    candidate.setPreviousMR(previousCandidates.get(j));
                }
            }
            filterPs[i] = maxTFp * eps[i];
            if (filterPs[i] > 0.0) {
                allZero = false;
            }
        }
        // 如果过滤概率全为0, 将发射概率视作过滤概率(说明通路不存在需要重新开启route匹配, 将缓存结果加入结果)
        if (allZero) {
            filterPs = eps;
            // 道路中断将缓存结果加入流
            if (previousIndex >= 0) {
                pushIntoStream(collector, previousCandidates.get(previousIndex));
            }
        }
        // 获取过滤概率最高的候选点
        double maxP = 0.0;
        int validCount = 0, bestIndex = 0;
        mr = candidates.get(0);
        for (int i = 0; i < candidates.size(); i++) {
            MatchingResult candidate = candidates.get(i);
            if (allZero) {
                candidate.setRouteStart(true);
                candidate.setPreviousMR(null);
            }
            if (filterPs[i] > 0) {
                validCount++;
            }
            if (filterPs[i] > maxP) {
                maxP = filterPs[i];
                bestIndex = i;
                mr = candidate;
            }
        }

        // 去除累积概率为0或者与局部最优概率差距过大的候选点, 减轻后续运算压力
        previousFps = new double[validCount];
        int fi = 0, tmp = bestIndex;
        for (int i = 0; i < filterPs.length; i++) {
            if (filterPs[i] > 0 && canRetain(maxP, filterPs[i])) {
                // 将过滤概率等比扩大防止精度不足导致的损失
                previousFps[fi++] = filterPs[i] * (1 / maxP);
            } else {
                if (i <= bestIndex) {
                    tmp--;
                }
                // 移除过滤概率为0的候选点, 减少后续运算压力
                candidates.remove(i - (filterPs.length - candidates.size()));
            }
        }
        bestIndex = tmp;
        // 经过排除之后道路匹配结果唯一将缓存结果加入流
        if (candidates.size() == 1) {
            if (previousIndex >= 0) {
                pushIntoStream(collector, previousCandidates.get(previousIndex));
            }
            mr.setInStream(true);
            collector.collect(mr);
        }
        // 更新变量状态
        filterProbabilitiesState.update(previousFps);
        candidatesState.update(candidates);
        gpsPointState.update(gpsPoint);
        bestIndexState.update(bestIndex);
    }

    protected void pushIntoStream(Collector<MatchingResult> collector, MatchingResult lastMR) {
        Deque<MatchingResult> stack = new LinkedList<>();
        while (lastMR != null && !lastMR.isInStream()) {
            stack.push(lastMR);
            lastMR.setInStream(true);
            lastMR = lastMR.getPreviousMR();
        }
        // 实验测试用
        if (stack.size() > 0) {
            log.debug("Batch Push Size: {}", stack.size());
//            MatchingTest.BATCH_COUNT_LIST.add(stack.size());
        }
        while (!stack.isEmpty()) {
            MatchingResult mr = stack.poll();
            // 中断上游联系, 防止路径过长堆栈溢出
            mr.setPreviousMR(null);
            collector.collect(mr);
        }
    }

    protected boolean canRetain(double max, double v) {
        return (max / v) <= quotient;
    }
}
