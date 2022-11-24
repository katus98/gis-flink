package com.katus;

import cn.edu.zju.gis.td.common.collection.Tuple;
import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import cn.edu.zju.gis.td.common.util.Strings;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;

/**
 * @author SUN Katus
 * @version 1.0, 2022-11-23
 */
@Slf4j
public class RoadsInfoProcessing {
    public static void main(String[] args) throws IOException {
        joinNetRoad();
    }

    /**
     * 将道路连接到net_roads上的ID
     */
    private static void joinNetRoad() throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        LineIterator it = fsManipulator.getLineIterator("E:\\graduation-project\\data\\roads_ori\\car_roads_points_Jinhua_join.csv");
        Map<String, Tuple<String, String>> map = new HashMap<>();
        while (it.hasNext()) {
            String line = it.next();
            String[] items = line.split(",");
            if (items.length == 1) {
                map.put(items[0], new Tuple<>("", ""));
            } else if (items.length == 2) {
                map.put(items[0], new Tuple<>(items[1], ""));
            } else {
                map.put(items[0], new Tuple<>(items[1], items[2]));
            }
        }
        it = fsManipulator.getLineIterator("E:\\graduation-project\\data\\roads_ori\\roads_net_Jinhua_join.csv");
        List<String> contents = new ArrayList<>();
        while (it.hasNext()) {
            String line = it.next();
            String[] items = line.split(",");
            Tuple<String, String> tuple = map.getOrDefault(items[1], new Tuple<>("", ""));
            contents.add(items[0] + "," + tuple._1() + "," + tuple._2());
        }
        fsManipulator.writeTextToFile("E:\\graduation-project\\data\\roads_ori\\net_Jinhua_join.csv", contents);
    }

    /**
     * 处理路名匹配的结果
     */
    private static void roadInfoProcess() throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        LineIterator it = fsManipulator.getLineIterator("E:\\graduation-project\\data\\roads_ori\\car_roads_points_Jinhua_res1.csv");
        StringBuilder builder = new StringBuilder();
        while (it.hasNext()) {
            String line = it.next();
            String[] items = line.split(",");
            builder.append(items[0]).append(",").append(items[1]).append(",");
            builder.append(items[2]).append(",").append(items[3]).append(",");
            if (items.length > 8) {
                if (items[8].contains("(")) {
                    String[] is = items[8].split("\\(");
                    builder.append(is[1].replace(")", "")).append(',').append(is[0]);
                } else if (Strings.allLetterDigit(items[8])) {
                    builder.append(',').append(items[8]);
                } else {
                    builder.append(items[8]).append(',');
                }
            } else {
                builder.append(",");
            }
            builder.append('\n');
        }
        fsManipulator.writeTextToFile("E:\\graduation-project\\data\\roads_ori\\car_roads_points_Jinhua_final.csv", Collections.singletonList(builder.toString()));
    }
}
