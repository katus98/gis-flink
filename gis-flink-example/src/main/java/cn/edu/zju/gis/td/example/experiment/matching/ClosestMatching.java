package cn.edu.zju.gis.td.example.experiment.matching;

import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.MatchingResult;
import cn.edu.zju.gis.td.example.experiment.global.GlobalUtil;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

import java.util.List;

/**
 * 最近道路匹配算法
 * 计算GPS点最近的道路, 如果是双行道则通过点位偏向确定方向
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-05
 */
public class ClosestMatching implements Matching<GpsPoint, MatchingResult> {

    @Override
    public boolean isCompatible(GpsPoint gpsPoint) {
        return true;
    }

    @Override
    public String name() {
        return "closest-matching";
    }

    @Override
    public MatchingResult map(GpsPoint gpsPoint) throws Exception {
        if (!isCompatible(gpsPoint)) {
            return null;
        }
        List<MatchingResult> candidates = MatchingSQL.queryNearCandidates(gpsPoint, 2);
        MatchingResult firstMR = candidates.get(0);
        switch (candidates.size()) {
            case 1:
                firstMR.update();
                return firstMR;
            case 2:
                if (firstMR.getEdgeWithInfo().isOneway()) {
                    firstMR.update();
                    return firstMR;
                }
                if (isRightBias(firstMR)) {
                    firstMR.update();
                    return firstMR;
                } else {
                    candidates.get(1).update();
                    return candidates.get(1);
                }
        }
        return null;
    }

    private static boolean isRightBias(MatchingResult matchingResult) {
        matchingResult.update();
        double d1 = GlobalUtil.calDirection(matchingResult.getMatchingSegment());
        GeometryFactory factory = JTSFactoryFinder.getGeometryFactory();
        LineString line = factory.createLineString(new Coordinate[]{matchingResult.getMatchingPoint().getCoordinate(), matchingResult.getOriginalPoint().getCoordinate()});
        double d2 = GlobalUtil.calDirection(line);
        return (d2 - d1 >= 0 && d2 - d1 < 180) || (d2 + 360 - d1 >= 0 && d2 + 360 - d1 < 180);
    }
}
