package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.ModelConstants;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;

import java.util.List;

/**
 * 最近道路方向匹配算法
 * 计算GPS方向吻合的最近道路分割
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-07
 */
public class ClosestDirectionMatching implements Matching<GpsPoint, MatchingResult> {

    @Override
    public boolean isCompatible(GpsPoint gpsPoint) {
        return gpsPoint.getDirect() >= 0.1;
    }

    @Override
    public String name() {
        return "closest-direction-matching";
    }

    @Override
    public MatchingResult map(GpsPoint gpsPoint) throws Exception {
        if (!isCompatible(gpsPoint)) {
            return new ClosestMatching().map(gpsPoint);
        }
        List<MatchingResult> candidates = QueryUtil.queryNearCandidates(gpsPoint);
        // 如果不存在候选点直接结束匹配
        if (candidates.isEmpty()) {
            return null;
        }
        for (MatchingResult mr : candidates) {
            if (judgeDirections(mr)) {
                mr.update();
                return mr;
            }
        }
        // 如果没有符合条件的方向匹配点则退化为最近道路匹配
        return new ClosestMatching().map(gpsPoint);
    }

    protected boolean judgeDirections(MatchingResult matchingResult) {
        List<Double> directions = matchingResult.getEdgeWithInfo().getDirections();
        for (Double direction : directions) {
            if (judgeDirection(direction, matchingResult.getGpsPoint().getDirect())) {
                return true;
            }
        }
        return false;
    }

    protected boolean judgeDirection(double d1, double d2) {
        double tmp = Math.abs(d1 - d2);
        return tmp <= 1.5 * ModelConstants.DIR_STANDARD_DEVIATION || Math.abs(tmp - 360) <= 1.5 * ModelConstants.DIR_STANDARD_DEVIATION;
    }
}
