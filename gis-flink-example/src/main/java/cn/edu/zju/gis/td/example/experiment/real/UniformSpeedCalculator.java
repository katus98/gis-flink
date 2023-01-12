package cn.edu.zju.gis.td.example.experiment.real;

import cn.edu.zju.gis.td.example.experiment.entity.LocationType;
import cn.edu.zju.gis.td.example.experiment.entity.MatPoint;
import cn.edu.zju.gis.td.example.experiment.entity.RealTimeStopInfo;
import cn.edu.zju.gis.td.example.experiment.entity.StopInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * 匀速运动模型
 * * 默认匹配点之间速度恒定
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-11
 */
@Slf4j
public class UniformSpeedCalculator extends RealTimeInfoCalculator {
    public UniformSpeedCalculator(LocationType locationType) {
        super(locationType);
    }

    public UniformSpeedCalculator() {
        super(LocationType.EDGE);
    }

    @Override
    public List<RealTimeStopInfo> computeRealTimeInfo(List<StopInfo> stops, MatPoint previousMP, MatPoint matPoint, double totalCost) {
        List<RealTimeStopInfo> list = new ArrayList<>();
        double accCost = 0.0;
        long curTime = previousMP.getTimestamp();
        long deltaTime = matPoint.getTimestamp() - curTime;
        double speed = (totalCost / (deltaTime / 1000.0)) * 3.6;
        for (StopInfo stop : stops) {
            double cost = stop.getCost() - accCost;
            double time;
            if (totalCost > 0.0) {
                time = deltaTime * (cost / totalCost);
            } else {
                time = deltaTime;
            }
            curTime += time;
            list.add(new RealTimeStopInfo(stop.getId(), matPoint.getTaxiId(), curTime, speed));
            accCost = stop.getCost();
        }
        // 对于边类型的最后一段不进行流量统计
        if (list.size() > 1 && (LocationType.EDGE.equals(locationType) || LocationType.CENTER_POINT.equals(locationType))) {
            list.get(stops.size() - 1).delFlow();
        }
        return list;
    }
}
