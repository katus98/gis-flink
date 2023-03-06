package com.katus.chart;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author SUN Katus
 * @version 1.0, 2023-02-21
 */
@Slf4j
public class DelayTest {
    public static void main(String[] args) throws IOException {
        merge("E:\\data\\graduation\\matchingViz\\cached-present-hidden-markov-matching-y-5-10");
        merge("E:\\data\\graduation\\matchingViz\\closest-direction-accurate-matching");
        merge("E:\\data\\graduation\\matchingViz\\closest-direction-matching");
        merge("E:\\data\\graduation\\matchingViz\\closest-matching");
        merge("E:\\data\\graduation\\matchingViz\\closest-path-matching");
        merge("E:\\data\\graduation\\matchingViz\\global-hidden-markov-matching");
        merge("E:\\data\\graduation\\matchingViz\\incremental-hidden-markov-matching");
        merge("E:\\data\\graduation\\matchingViz\\present-hidden-markov-matching");
    }

    private static void delay() throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        String[] files = fsManipulator.list("E:\\data\\graduation\\gpsFilter");
        long lastT, total = 0L, count = 0L;
        for (String file : files) {
            log.info("START FILE {}", file);
            LineIterator iterator = fsManipulator.getLineIterator(file);
            lastT = -1L;
            while (iterator.hasNext()) {
                String line = iterator.next();
                String[] items = line.split(",");
                long t = Long.parseLong(items[8]);
                if (lastT != -1L && t - lastT <= 180000) {
                    total += t - lastT;
                    count++;
                }
                lastT = t;
            }
        }
        double result = 1.0 * total / count / 1000;
        log.info("AVG ITV {}s", result);
    }

    private static void merge(String path) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        String[] files = fsManipulator.list(path);
        Arrays.sort(files);
        List<String> content = new ArrayList<>();
        for (String file : files) {
            LineIterator it = fsManipulator.getLineIterator(file);
            while (it.hasNext()) {
                String line = it.next();
                content.add(line);
            }
        }
        fsManipulator.writeTextToFile(path + ".csv", content);
    }
}
