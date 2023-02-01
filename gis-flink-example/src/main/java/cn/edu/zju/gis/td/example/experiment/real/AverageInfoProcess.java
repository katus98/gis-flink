package cn.edu.zju.gis.td.example.experiment.real;

import cn.edu.zju.gis.td.example.experiment.entity.AverageLocationInfo;
import cn.edu.zju.gis.td.example.experiment.entity.LocationType;
import cn.edu.zju.gis.td.example.experiment.entity.RealTimeStopInfo;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 交通量时间窗口处理器
 *
 * @author SUN Katus
 * @version 1.0, 2023-02-01
 */
@Slf4j
public class AverageInfoProcess extends ProcessWindowFunction<RealTimeStopInfo, AverageLocationInfo, Long, TimeWindow> {
    private final LocationType locationType;

    public AverageInfoProcess(LocationType locationType) {
        if (LocationType.EDGE.equals(locationType) || LocationType.CENTER_POINT.equals(locationType)) {
            this.locationType = locationType;
        } else {
            log.error("{} is NOT a proper type for AverageInfoCalculator.", locationType.name());
            throw new RuntimeException();
        }
    }

    @Override
    public void process(Long id, ProcessWindowFunction<RealTimeStopInfo, AverageLocationInfo, Long, TimeWindow>.Context context, Iterable<RealTimeStopInfo> iterable, Collector<AverageLocationInfo> collector) throws Exception {
        long count = 0L;
        double speed = 0.0, flow = 0.0;
        for (RealTimeStopInfo realTimeStopInfo : iterable) {
            speed += realTimeStopInfo.getSpeed();
            flow += realTimeStopInfo.getFlow();
            count++;
        }
        if (count > 0) {
            AverageLocationInfo info = new AverageLocationInfo(id, context.currentProcessingTime(), flow, speed / count);
            // 更新数据库交通量信息
            if (LocationType.EDGE.equals(locationType)) {
                QueryUtil.updateInfoToEdge(id, flow, info.getSpeed());
            } else {
                QueryUtil.updateInfoToAnaUnit(id, flow, info.getSpeed());
            }
            collector.collect(info);
            log.debug("{} : {}-{} flow = {}, speed = {}", info.getTimestamp(), locationType.name(), id, flow, info.getSpeed());
        } else {
            // 如果没有通过信息则重置速度
            if (LocationType.EDGE.equals(locationType)) {
                QueryUtil.resetEdgeSpeedById(id);
            } else {
                QueryUtil.resetAnaUnitSpeedById(id);
            }
        }
    }
}
