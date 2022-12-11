package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.GraphNode;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
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
        long deltaTime = gpsPoint.getTimestamp() - previousMR.getGpsPoint().getTimestamp();
        double radius = MatchingConstants.MAX_ALLOW_SPEED * (deltaTime / 1000.0) + 2 * MatchingConstants.GPS_TOLERANCE;
        List<MatchingResult> candidates = MatchingSQL.queryNearCandidates(gpsPoint);
        Set<Long> edgeIds = MatchingSQL.queryEdgeIdsWithinRange(previousMR.getMatchingPoint(), radius);
        Map<Long, GraphNode> nodeGraphMap = MatchingSQL.queryNodeIdsWithinRange(previousMR.getMatchingPoint(), radius);
        long startId = previousMR.getEdgeWithInfo().getEndId();
        for (Map.Entry<Long, GraphNode> entry : nodeGraphMap.entrySet()) {
            entry.setValue(new GraphNode(entry.getKey()));
        }
        GraphNode startGraphNode = nodeGraphMap.get(startId);
        startGraphNode.setCumulativeDistance(previousMR.getLengthToNextNode());
        // todo: 完成最短路径算法
        return mr;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.lastMatchingResultState = getRuntimeContext().getState(new ValueStateDescriptor<>("previous-matching-result", MatchingResult.class));
    }
}
