package cn.edu.zju.gis.td.example.experiment.real;

import cn.edu.zju.gis.td.example.experiment.entity.FlowAndSpeed;
import cn.edu.zju.gis.td.example.experiment.entity.MatPoint;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 实时流量速度计算器
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-02
 */
public class FlowAndSpeedCalculator extends RichFlatMapFunction<MatPoint, FlowAndSpeed> {

    @Override
    public void flatMap(MatPoint matPoint, Collector<FlowAndSpeed> collector) throws Exception {

    }
}
