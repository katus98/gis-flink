package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.ModelConstants;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 隐马尔可夫匹配算法
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-13
 */
public abstract class HiddenMarkovMatching extends RichFlatMapFunction<GpsPoint, MatchingResult> implements Matching<GpsPoint, MatchingResult> {
    protected transient ListState<MatchingResult> candidatesState;
    protected transient ValueState<double[]> filterProbabilitiesState;
    protected transient ValueState<GpsPoint> gpsPointState;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.candidatesState = getRuntimeContext().getListState(new ListStateDescriptor<>("candidates", MatchingResult.class));
        this.filterProbabilitiesState = getRuntimeContext().getState(new ValueStateDescriptor<>("filter-probabilities", double[].class));
        this.gpsPointState = getRuntimeContext().getState(new ValueStateDescriptor<>("gps-point", GpsPoint.class));
    }

    @Override
    public void flatMap(GpsPoint gpsPoint, Collector<MatchingResult> collector) throws Exception {
        collector.collect(map(gpsPoint));
    }

    protected double[] computeEmissionProbabilities(double[] errors) {
        double[] eps = new double[errors.length];
//        double sigma = 1.4826 * median(errors);
        for (int i = 0; i < eps.length; i++) {
            eps[i] = emissionProbability(errors[i], ModelConstants.DIS_STANDARD_DEVIATION);
        }
        return eps;
    }

    protected double[] computeTransitionProbabilities(double[] dts) {
        double[] tps = new double[dts.length];
        double beta = 1 / Math.log(2) * Math.max(median(dts), 1.0);
        for (int i = 0; i < tps.length; i++) {
            tps[i] = transitionProbability(dts[i], beta);
        }
        return tps;
    }

    protected double emissionProbability(double error, double sigma) {
        return 1.0 / (Math.sqrt(2 * Math.PI) * sigma) * Math.pow(Math.E, -0.5 * Math.pow(error / sigma, 2));
    }

    protected double transitionProbability(double dt, double beta) {
        return 1.0 / beta * Math.pow(Math.E, -dt / beta);
    }

    private double average(double[] array) {
        double avg = 0.0;
        int count = 0;
        for (double v : array) {
            if (v < ModelConstants.MAX_COST / 10) {
                avg += v;
                count++;
            }
        }
        return count == 0 ? 0.0 : avg / count;
    }

    private double median(double[] array) {
        int c = 0;
        for (double v : array) {
            if (v < ModelConstants.MAX_COST / 10) {
                c++;
            }
        }
        double[] nArray = new double[c];
        c = 0;
        for (double v : array) {
            if (v < ModelConstants.MAX_COST / 10) {
                nArray[c++] = v;
            }
        }
        Arrays.sort(nArray);
        c = nArray.length;
        if (c == 0) return 0;
        return c % 2 == 1 ? nArray[(c - 1) / 2] : (nArray[c / 2 - 1] + nArray[c / 2]) / 2.0;
    }
}
