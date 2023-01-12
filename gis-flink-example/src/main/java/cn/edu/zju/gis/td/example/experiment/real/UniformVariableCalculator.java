package cn.edu.zju.gis.td.example.experiment.real;

import cn.edu.zju.gis.td.example.experiment.entity.LocationType;
import cn.edu.zju.gis.td.example.experiment.entity.MatPoint;
import cn.edu.zju.gis.td.example.experiment.entity.RealTimeStopInfo;
import cn.edu.zju.gis.td.example.experiment.entity.StopInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * 变速运动模型
 * * 尚未考虑出实现细节
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-11
 */
@Slf4j
public class UniformVariableCalculator extends RealTimeInfoCalculator {
    public UniformVariableCalculator(LocationType locationType) {
        super(locationType);
    }

    public UniformVariableCalculator() {
        super(LocationType.EDGE);
    }

    @Override
    public List<RealTimeStopInfo> computeRealTimeInfo(List<StopInfo> stops, MatPoint previousMP, MatPoint matPoint, double totalCost) {
        List<RealTimeStopInfo> list = new ArrayList<>();
        // todo
        return list;
    }
}
