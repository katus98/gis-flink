package cn.edu.zju.gis.td.example.experiment.real;

import cn.edu.zju.gis.td.example.experiment.entity.*;
import cn.edu.zju.gis.td.example.experiment.global.ModelConstants;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

/**
 * 时间窗口内数据计算器
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-07
 */
@Slf4j
@Deprecated
public class AverageInfoCalculator extends RichMapFunction<RealTimeStopInfo, AverageLocationInfo> {
    private final LocationType locationType;
    private transient ValueState<LocationTaxis> locationTaxisState;

    public AverageInfoCalculator(LocationType locationType) {
        if (LocationType.EDGE.equals(locationType) || LocationType.CENTER_POINT.equals(locationType)) {
            this.locationType = locationType;
        } else {
            log.error("{} is NOT a proper type for AverageInfoCalculator.", locationType.name());
            throw new RuntimeException();
        }
    }

    @Override
    public void open(Configuration parameters) {
        this.locationTaxisState = getRuntimeContext().getState(new ValueStateDescriptor<>("location-taxis", LocationTaxis.class));
    }

    @Override
    public AverageLocationInfo map(RealTimeStopInfo stopInfo) throws Exception {
        LocationTaxis locationTaxis = locationTaxisState.value();
        // 如果是第一次更新的位置需要初始化状态信息
        if (locationTaxis == null) {
            if (LocationType.EDGE.equals(locationType)) {
                locationTaxis = QueryUtil.initEdgeLocationById(stopInfo.getId());
            } else {
                locationTaxis = QueryUtil.initAnaUnitLocationById(stopInfo.getId());
            }
        }
        // 计算时间窗口内的平均交通量
        long currentTime = stopInfo.getTimestamp();
        locationTaxis.addEvent(new TaxiEvent(stopInfo.getTaxiId(), currentTime, stopInfo.getFlow(), stopInfo.getSpeed()));
        locationTaxis.expire(currentTime, ModelConstants.TIME_WINDOW);
        double speed = locationTaxis.obtainAvgSpeed();
        double flow = locationTaxis.obtainFlowCount();
        // 更新数据库中的信息
        if (LocationType.EDGE.equals(locationType)) {
            QueryUtil.updateInfoToEdge(stopInfo.getId(), flow, speed);
        } else {
            QueryUtil.updateInfoToAnaUnit(stopInfo.getId(), flow, speed);
        }
        // 更新状态
        locationTaxisState.update(locationTaxis);
        log.debug("{} : {}-{} flow = {}, speed = {}", currentTime, locationType.name(), stopInfo.getId(), flow, speed);
        return new AverageLocationInfo(stopInfo.getId(), currentTime, flow, speed);
    }
}
