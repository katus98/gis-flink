package cn.edu.zju.gis.td.example.experiment.real;

import cn.edu.zju.gis.td.example.experiment.entity.*;
import cn.edu.zju.gis.td.example.experiment.global.GraphCalculator;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
import cn.edu.zju.gis.td.example.experiment.global.ModelConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 实时数据计算器
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-02
 */
@Slf4j
public abstract class RealTimeInfoCalculator<MAT extends Matchable> extends RichFlatMapFunction<MAT, RealTimeStopInfo> {
    protected final LocationType locationType;
    private transient ValueState<Matchable> matPointState;

    protected RealTimeInfoCalculator(LocationType locationType) {
        this.locationType = locationType;
    }

    @Override
    public void open(Configuration parameters) {
        this.matPointState = getRuntimeContext().getState(new ValueStateDescriptor<>("mat-point", Matchable.class));
    }

    @Override
    public void flatMap(MAT matPoint, Collector<RealTimeStopInfo> collector) throws Exception {
        Matchable previousMP = matPointState.value();
        if (previousMP != null && !matPoint.isRouteStart()) {
            // 计算与上一次匹配点的间隔时间
            long deltaTime = matPoint.getTimestamp() - previousMP.getTimestamp();
            // 计算时间间隔内的最大可能通行范围
            double radius = ModelConstants.MAX_ALLOW_SPEED * (deltaTime / 1000.0) + 2 * ModelConstants.GPS_TOLERANCE;
            // 获取范围内的所有边ID
            Set<Long> edgeIds = QueryUtil.queryEdgeIdsWithinRange(previousMP.getMatX(), previousMP.getMatY(), radius);
            // 获取范围内的所有节点ID
            Map<Long, GraphNode> nodeGraphMap = QueryUtil.queryNodeIdsWithinRange(previousMP.getMatX(), previousMP.getMatY(), radius);
            GraphCalculator calculator = new GraphCalculator(nodeGraphMap, edgeIds);
            calculator.setStartPoint(previousMP);
            // 根据位置类型计算对应的途经位置信息
            List<StopInfo> stops;
            switch (locationType) {
                case EDGE:
                    stops = calculator.acquireRouteAllEdgeStops(matPoint);
                    break;
                case CENTER_POINT:
                    stops = calculator.acquireRouteCenterPointStops(matPoint);
                    // 实际上中心点表示的实际意义是分析单元路段, 所以不能仅包括经过, 起点终点所在分析单元也需要包含
                    long id1 = QueryUtil.queryCenterPointIdWithGeometry(previousMP);
                    if (stops.isEmpty() || (id1 != -1L && stops.get(0).getId() != id1)) {
                        if (!stops.isEmpty()) {
                            double nCost = stops.get(0).getCost();
                            // 估算如果存在后续节点则成本为后者一半
                            stops.add(0, new StopInfo(id1, nCost / 2.0));
                        } else {
                            // 如果不存在后续成本暂时设置为0
                            stops.add(0, new StopInfo(id1, 0.0));
                        }
                    }
                    long id2 = QueryUtil.queryCenterPointIdWithGeometry(matPoint);
                    if (id2 != -1L && stops.get(stops.size() - 1).getId() != id2) {
                        stops.add(new StopInfo(id2, calculator.computeCost(matPoint)));
                    }
                    break;
                case REAL_NODE:
                    stops = calculator.acquireRouteRealNodeStops(matPoint);
                    break;
                case NODE:
                    stops = calculator.acquireRouteAllNodeStops(matPoint);
                    break;
                default:
                    stops = Collections.emptyList();
            }
            // 如果新位置可以由上个位置抵达, 则开始计算每个位置对应的速度
            if (calculator.canArrived(matPoint)) {
                List<RealTimeStopInfo> list = computeRealTimeInfo(stops, previousMP, matPoint, calculator.computeCost(matPoint));
                for (RealTimeStopInfo info : list) {
                    collector.collect(info);
                    log.debug("{} : TAXI-{} {}-{} flow = {}, speed = {}", info.getTimestamp(), matPoint.getTaxiId(), locationType.name(), info.getId(), info.getFlow(), info.getSpeed());
                }
            }
        }
        matPointState.update(matPoint);
    }

    public abstract List<RealTimeStopInfo> computeRealTimeInfo(List<StopInfo> stops, Matchable previousMP, MAT matPoint, double totalCost);
}
