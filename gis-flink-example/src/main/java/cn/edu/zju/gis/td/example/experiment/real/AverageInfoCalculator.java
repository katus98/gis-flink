package cn.edu.zju.gis.td.example.experiment.real;

import cn.edu.zju.gis.td.example.experiment.entity.*;
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
        if (locationTaxis == null) {
            if (LocationType.EDGE.equals(locationType)) {
                locationTaxis = QueryUtil.initEdgeLocationById(stopInfo.getId());
            } else {
                locationTaxis = QueryUtil.initAnaUnitsLocationById(stopInfo.getId());
            }
        }
        long currentTime = stopInfo.getTimestamp();
        locationTaxis.addEvent(new TaxiEvent(stopInfo.getTaxiId(), currentTime, stopInfo.getSpeed()));
        locationTaxis.expire(currentTime, 10800000L);
        double speed = locationTaxis.obtainAvgSpeed();
        double flow = locationTaxis.obtainFlowCount();
        // todo 更新数据库中的信息
        locationTaxisState.update(locationTaxis);
        return new AverageLocationInfo(stopInfo.getId(), currentTime, flow, speed);
    }
}
