package com.katus;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import cn.edu.zju.gis.td.common.util.IOUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Writer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-01
 */
@Slf4j
public class TrafficProcessing {
    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
        mergeSort("D:\\data\\graduation\\traffic_ori\\accidents", true, 1,
                "D:\\data\\graduation\\traffic_ori\\accidents_geo_all.csv");
        mergeSort("D:\\data\\graduation\\traffic_ori\\illegal\\illegal_2019_geo.csv", true, 2,
                "D:\\data\\graduation\\traffic_ori\\illegal_2019_sort.csv");
        mergeSort("D:\\data\\graduation\\traffic_ori\\illegal\\illegal_2020_geo.csv", true, 2,
                "D:\\data\\graduation\\traffic_ori\\illegal_2020_sort.csv");
        mergeSort("D:\\data\\graduation\\traffic_ori\\illegal\\illegal_2021_geo.csv", true, 2,
                "D:\\data\\graduation\\traffic_ori\\illegal_2021_sort.csv");
        mergeSort("D:\\data\\graduation\\traffic_ori\\illegal\\illegal_2022_geo.csv", true, 2,
                "D:\\data\\graduation\\traffic_ori\\illegal_2022_sort.csv");
        mergeDir("D:\\data\\graduation\\traffic_ori\\illegal", "D:\\data\\graduation\\traffic_ori\\illegal_all.csv");
    }

    /**
     * 按照文件名关键字进行文件合并
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

    /**
     * 合并文件并按照时间排序
     */
    private static void mergeSort(String inputPath, boolean title, int timeIndex, String outfile) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        String[] filenames;
        if (fsManipulator.isDirectory(inputPath)) {
            filenames = fsManipulator.list(inputPath);
        } else {
            filenames = new String[]{inputPath};
        }
        TreeMap<Long, List<String>> treeMap = new TreeMap<>();
        for (String filename : filenames) {
            log.info("START FILE: [{}]", filename);
            LineIterator it = fsManipulator.getLineIterator(filename);
            if (title) {
                it.next();
            }
            long count = 0L;
            while (it.hasNext()) {
                String line = it.next();
                String[] items = line.split(",");
                try {
                    long key = FORMAT.parse(items[timeIndex]).getTime();
                    if (treeMap.containsKey(key)) {
                        treeMap.get(key).add(line);
                    } else {
                        List<String> list = new ArrayList<>();
                        list.add(line);
                        treeMap.put(key, list);
                    }
                } catch (ParseException e) {
                    log.warn("CONTEXT: [{}] IS NOT VALID!", line);
                }
                count++;
                if (count % 100000 == 0) {
                    log.info("FINISH COUNT {}", count);
                }
            }
            log.info("FINISH FILE: [{}]", filename);
        }
        List<String> content = new ArrayList<>();
        for (Map.Entry<Long, List<String>> entry : treeMap.entrySet()) {
            content.addAll(entry.getValue());
        }
        fsManipulator.writeTextToFile(outfile, content);
        log.info("ALL FINISHED! TOTAL = {}", content.size());
    }

    /**
     * 合并目录下的所有文件
     */
    private static void mergeDir(String dirPath, String outfile) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        String[] filenames = fsManipulator.list(dirPath);
        Writer writer = fsManipulator.writeAsText(outfile);
        long count = 0L;
        for (String filename : filenames) {
            log.info("START FILE: [{}]", filename);
            LineIterator it = fsManipulator.getLineIterator(filename);
            while (it.hasNext()) {
                String line = it.next() + "\n";
                writer.write(line);
                count++;
                if (count % 100000 == 0) {
                    log.info("FINISH COUNT {}", count);
                }
            }
            log.info("FINISH FILE: [{}]", filename);
        }
        IOUtils.closeAll(writer);
        log.info("ALL FINISHED! TOTAL = {}", count);
    }
}
