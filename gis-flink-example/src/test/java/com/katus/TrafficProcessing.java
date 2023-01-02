package com.katus;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-01
 */
@Slf4j
public class TrafficProcessing {
    public static void main(String[] args) throws IOException {
        mergeDirFiles("D:\\data\\graduation\\traffic_ori\\金华市交通数据处理\\csv\\accidents",
                "D:\\data\\graduation\\traffic_ori\\金华市交通数据处理\\csv\\accidents.csv",
                "");
        mergeDirFiles("D:\\data\\graduation\\traffic_ori\\金华市交通数据处理\\csv\\illegal",
                "D:\\data\\graduation\\traffic_ori\\金华市交通数据处理\\csv\\illegal_2019.csv",
                "2019");
        mergeDirFiles("D:\\data\\graduation\\traffic_ori\\金华市交通数据处理\\csv\\illegal",
                "D:\\data\\graduation\\traffic_ori\\金华市交通数据处理\\csv\\illegal_2020.csv",
                "2020");
        mergeDirFiles("D:\\data\\graduation\\traffic_ori\\金华市交通数据处理\\csv\\illegal",
                "D:\\data\\graduation\\traffic_ori\\金华市交通数据处理\\csv\\illegal_2021.csv",
                "2021");
        mergeDirFiles("D:\\data\\graduation\\traffic_ori\\金华市交通数据处理\\csv\\illegal",
                "D:\\data\\graduation\\traffic_ori\\金华市交通数据处理\\csv\\illegal_2022.csv",
                "2022");
    }

    /**
     * 文件合并
     */
    private static void mergeDirFiles(String input, String output, String keyword) throws IOException {
        log.info("--- {} ---", output);
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        String[] files = fsManipulator.list(input);
        List<String> content = new ArrayList<>();
        for (String file : files) {
            if (!keyword.isEmpty() && !file.contains(keyword)) {
                continue;
            }
            log.info("{}", file);
            LineIterator it = fsManipulator.getLineIterator(file);
            String title = it.next();
            if (content.isEmpty()) {
                content.add(title);
            }
            while (it.hasNext()) {
                String line = it.next();
                // 29 32
                if (line.split(",").length != 32 || !line.endsWith("-")) {
                    log.warn("FILE: {} EXISTS ERROR!", file);
                    log.warn("CONTENT: {}", line);
                    continue;
                }
                content.add(line);
            }
        }
        fsManipulator.writeTextToFile(output, content);
    }
}
