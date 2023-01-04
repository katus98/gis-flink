package cn.edu.zju.gis.td.example.experiment.real;

import cn.edu.zju.gis.td.example.experiment.entity.FlowAndSpeed;
import cn.edu.zju.gis.td.example.experiment.entity.GraphNode;
import cn.edu.zju.gis.td.example.experiment.entity.MatPoint;
import cn.edu.zju.gis.td.example.experiment.global.GraphCalculator;
import cn.edu.zju.gis.td.example.experiment.matching.MatchingConstants;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Set;

/**
 * 实时流量速度计算器
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-02
 */
public class FlowAndSpeedCalculator extends RichFlatMapFunction<MatPoint, FlowAndSpeed> {
    private transient ValueState<MatPoint> matPointState;

    @Override
    public void open(Configuration parameters) {
        this.matPointState = getRuntimeContext().getState(new ValueStateDescriptor<>("mat-point", MatPoint.class));
    }

    @Override
    public void flatMap(MatPoint matPoint, Collector<FlowAndSpeed> collector) throws Exception {
        MatPoint previousMP = matPointState.value();
        if (previousMP != null && !matPoint.isRouteStart()) {
            // 计算与上一次匹配点的间隔时间
            long deltaTime = matPoint.getTimestamp() - previousMP.getTimestamp();
            // 计算时间间隔内的最大可能通行范围
            double radius = MatchingConstants.MAX_ALLOW_SPEED * (deltaTime / 1000.0) + 2 * MatchingConstants.GPS_TOLERANCE;
            // 获取范围内的所有边ID
            Set<Long> edgeIds = QueryUtil.queryEdgeIdsWithinRange(previousMP.getMatX(), previousMP.getMatY(), radius);
            // 获取范围内的所有节点ID
            Map<Long, GraphNode> nodeGraphMap = QueryUtil.queryNodeIdsWithinRange(previousMP.getMatX(), previousMP.getMatY(), radius);
            GraphCalculator calculator = new GraphCalculator(nodeGraphMap, edgeIds);
            calculator.setStartPoint(previousMP);
            double cost = calculator.computeCost(matPoint);
            // todo 获取路径上的结点ID
        }
        matPointState.update(matPoint);
    }
}
