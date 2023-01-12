package cn.edu.zju.gis.td.example.experiment.real;

import cn.edu.zju.gis.td.example.experiment.entity.RealTimeStopInfo;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.ModelConstants;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-11
 */
public class CheckPointSource implements SourceFunction<RealTimeStopInfo> {
    private boolean isRunning;
    private int count;

    public CheckPointSource() {
        this.isRunning = true;
        this.count = 0;
    }

    @Override
    public void run(SourceContext<RealTimeStopInfo> sourceContext) throws Exception {
        while (isRunning) {
            for (int i = 1; i <= ModelConstants.CENTER_POINT_NUMBER; i++) {
                sourceContext.collect(RealTimeStopInfo.generateCheckInfo(i, GlobalConfig.TIME_0501 + count * ModelConstants.TIME_WINDOW));
            }
            this.count++;
            Thread.sleep(ModelConstants.TIME_WINDOW);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
