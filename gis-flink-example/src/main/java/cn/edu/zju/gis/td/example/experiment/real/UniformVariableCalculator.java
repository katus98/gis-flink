package cn.edu.zju.gis.td.example.experiment.real;

import cn.edu.zju.gis.td.example.experiment.entity.*;
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
public class UniformVariableCalculator<MAT extends Matchable> extends RealTimeInfoCalculator<MAT> {
    public UniformVariableCalculator(LocationType locationType) {
        super(locationType);
    }

    public UniformVariableCalculator() {
        super(LocationType.EDGE);
    }

    @Override
    public List<RealTimeStopInfo> computeRealTimeInfo(List<StopInfo> stops, Matchable previousMP, MAT matPoint, double totalCost) {
        List<RealTimeStopInfo> list = new ArrayList<>();
        // todo
        return list;
    }
}
