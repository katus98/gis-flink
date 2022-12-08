package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.GlobalUtil;

/**
 * 最近道路方向精确匹配算法
 * 计算GPS方向吻合的最近道路拐点分段
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-08
 */
public class ClosestDirectionAccurateMatching extends ClosestDirectionMatching {
    @Override
    protected boolean judgeDirections(MatchingResult matchingResult) {
        matchingResult.update();
        return judgeWithoutCache(matchingResult);
    }

    private boolean judgeWithCache(MatchingResult matchingResult) {
        return judgeDirection(matchingResult.getGpsPoint().getDirect(), matchingResult.getEdgeWithInfo().getDirections().get(matchingResult.getSegmentNo()));
    }

    private boolean judgeWithoutCache(MatchingResult matchingResult) {
        return judgeDirection(matchingResult.getGpsPoint().getDirect(), GlobalUtil.calDirection(matchingResult.getMatchingSegment()));
    }
}
