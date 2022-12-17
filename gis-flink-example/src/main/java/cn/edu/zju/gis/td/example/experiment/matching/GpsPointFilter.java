package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-17
 */
public class GpsPointFilter extends RichFilterFunction<GpsPoint> {
    private transient ValueState<GpsPoint> gpsPointState;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.gpsPointState = getRuntimeContext().getState(new ValueStateDescriptor<>("gps-point", GpsPoint.class));
    }

    @Override
    public boolean filter(GpsPoint gpsPoint) throws Exception {
        GpsPoint previousGpsPoint = gpsPointState.value();
        if (previousGpsPoint == null) {
            gpsPointState.update(gpsPoint);
            return true;
        }
        if (gpsPoint.usefulValueEquals(previousGpsPoint)) {
            return false;
        }
        if (gpsPoint.posEquals(previousGpsPoint)) {
            return gpsPoint.getTimestamp() - previousGpsPoint.getTimestamp() <= MatchingConstants.MAX_FILTER_DELTA_TIME;
        }
        gpsPointState.update(gpsPoint);
        return true;
    }
}
