package cn.edu.zju.gis.td.example.experiment.chart;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import cn.edu.zju.gis.td.example.experiment.entity.GraphNode;
import cn.edu.zju.gis.td.example.experiment.entity.MatPoint;
import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import cn.edu.zju.gis.td.example.experiment.global.GraphCalculator;
import cn.edu.zju.gis.td.example.experiment.global.ModelConstants;
import cn.edu.zju.gis.td.example.experiment.global.QueryUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author SUN Katus
 * @version 1.0, 2023-02-22
 */
@Slf4j
public class PathTest {
    public static void main(String[] args) throws IOException, SQLException {
        String basePath = args[0];
        Map<String, Double> map = new HashMap<>();
        map.put("global-hidden-markov-matching", mergeAndCal(basePath + "global-hidden-markov-matching"));
        map.put("closest-matching", mergeAndCal(basePath + "closest-matching"));
        map.put("closest-direction-matching", mergeAndCal(basePath + "closest-direction-matching"));
        map.put("closest-direction-accurate-matching", mergeAndCal(basePath + "closest-direction-accurate-matching"));
        map.put("closest-path-matching", mergeAndCal(basePath + "closest-path-matching"));
        map.put("incremental-hidden-markov-matching", mergeAndCal(basePath + "incremental-hidden-markov-matching"));
        map.put("present-hidden-markov-matching", mergeAndCal(basePath + "present-hidden-markov-matching"));
        map.put("cached-present-hidden-markov-matching-y-5-10", mergeAndCal(basePath + "cached-present-hidden-markov-matching-y-5-10"));
        for (Map.Entry<String, Double> entry : map.entrySet()) {
            log.info("{} : {}", entry.getKey(), entry.getValue());
        }
    }

    private static double mergeAndCal(String path) throws IOException, SQLException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        String[] filenames = fsManipulator.list(path);
        Arrays.sort(filenames);
        MatPoint lastP = null, matPoint;
        double distance = 0.0;
        int count = 0, noRoute = 0;
        for (String filename : filenames) {
            log.info(filename);
            LineIterator it = fsManipulator.getLineIterator(filename);
            while (it.hasNext()) {
                String line = it.next();
                String[] items = line.split(",");
                SerializedData.MatPointSer matPointSer = SerializedData.MatPointSer.newBuilder()
                        .setId(Long.parseLong(items[0])).setTaxiId(Integer.parseInt(items[1]))
                        .setOriX(Double.parseDouble(items[10])).setOriY(Double.parseDouble(items[11]))
                        .setMatX(Double.parseDouble(items[12])).setMatY(Double.parseDouble(items[13]))
                        .setEdgeId(Long.parseLong(items[9])).setRatioToNextNode(Double.parseDouble(items[15]))
                        .setRouteStart(Boolean.parseBoolean(items[14])).setTimestamp(Long.parseLong(items[8]))
                        .setSpeed(Double.parseDouble(items[4])).build();
                matPoint = new MatPoint(matPointSer);
                if (matPoint.getRatioToNextNode() > 1) {
                    matPoint.setRatioToNextNode(1.0);
                }
                if (matPoint.getRatioToNextNode() < 0) {
                    matPoint.setRatioToNextNode(0.0);
                }
                if (lastP != null && !matPoint.isRouteStart()) {
                    // 计算与上一次匹配点的间隔时间
                    long deltaTime = matPoint.getTimestamp() - lastP.getTimestamp();
                    // 计算时间间隔内的最大可能通行范围
                    double radius = ModelConstants.MAX_ALLOW_SPEED * (deltaTime / 1000.0) + 2 * ModelConstants.GPS_TOLERANCE;
                    // 获取范围内的所有边ID
                    Set<Long> edgeIds = QueryUtil.queryEdgeIdsWithinRange(lastP.getMatX(), lastP.getMatY(), radius);
                    // 获取范围内的所有节点ID
                    Map<Long, GraphNode> nodeGraphMap = QueryUtil.queryNodeIdsWithinRange(lastP.getMatX(), lastP.getMatY(), radius);
                    // 构建图计算器
                    GraphCalculator calculator = new GraphCalculator(nodeGraphMap, edgeIds);
                    calculator.setStartPoint(lastP);
                    if (calculator.canArrived(matPoint)) {
                        distance += calculator.computeCost(matPoint);
                    } else {
                        noRoute++;
                    }
                }
                lastP = matPoint;
                count++;
                if (count % 1000 == 0) {
                    log.info("{}: {} FINISHED!", path, count);
                }
            }
        }
        log.info("{} : {} {}", path, distance, noRoute);
        return distance;
    }
}
