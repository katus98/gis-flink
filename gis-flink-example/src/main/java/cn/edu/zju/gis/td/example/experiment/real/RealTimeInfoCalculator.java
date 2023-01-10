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
public class RealTimeInfoCalculator extends RichFlatMapFunction<MatPoint, RealTimeStopInfo> {
    private final LocationType locationType;
    private transient ValueState<MatPoint> matPointState;

    public RealTimeInfoCalculator(LocationType locationType) {
        this.locationType = locationType;
    }

    public RealTimeInfoCalculator() {
        this(LocationType.EDGE);
    }

    @Override
    public void open(Configuration parameters) {
        this.matPointState = getRuntimeContext().getState(new ValueStateDescriptor<>("mat-point", MatPoint.class));
    }

    @Override
    public void flatMap(MatPoint matPoint, Collector<RealTimeStopInfo> collector) throws Exception {
        MatPoint previousMP = matPointState.value();
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
                    // todo 针对中心点特别处理 分别根据车的前后两个位置查询对应的分析单元ID添加到经停中
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
                double totalCost = calculator.computeCost(matPoint);
                double accCost = 0.0;
                long curTime = previousMP.getTimestamp();
                for (StopInfo stop : stops) {
                    double cost = stop.getCost() - accCost;
                    double time;
                    if (totalCost > 0.0) {
                        time = deltaTime * (cost / totalCost);
                    } else {
                        time = deltaTime;
                    }
                    curTime += time;
                    double speed = cost / (time / 1000.0);
                    collector.collect(new RealTimeStopInfo(stop.getId(), matPoint.getTaxiId(), curTime, speed));
                    accCost = stop.getCost();
                    log.debug("{} : TAXI-{} {}-{} speed = {}", curTime, matPoint.getTaxiId(), locationType.name(), stop.getId(), speed);
                }
            }
        }
        matPointState.update(matPoint);
    }
}
