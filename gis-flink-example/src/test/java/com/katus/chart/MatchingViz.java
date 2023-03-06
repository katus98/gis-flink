package com.katus.chart;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author SUN Katus
 * @version 1.0, 2023-02-22
 */
@Slf4j
public class MatchingViz {
    public static void main(String[] args) throws IOException {
        merge("E:\\data\\graduation\\matching\\cached-present-hidden-markov-matching-y-5-10", "E:\\data\\graduation\\matchingViz\\cached-present-hidden-markov-matching-y-5-10.csv");
        merge("E:\\data\\graduation\\matching\\global-hidden-markov-matching", "E:\\data\\graduation\\matchingViz\\global-hidden-markov-matching.csv");
        merge("E:\\data\\graduation\\matching\\present-hidden-markov-matching", "E:\\data\\graduation\\matchingViz\\present-hidden-markov-matching.csv");
    }

    private static void merge(String path, String out) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        String[] filenames = fsManipulator.list(path);
        List<String> content = new ArrayList<>();
        for (String filename : filenames) {
            log.info(filename);
            LineIterator it = fsManipulator.getLineIterator(filename);
            while (it.hasNext()) {
                content.add(it.next());
            }
        }
        fsManipulator.writeTextToFile(out, content);
    }
}
