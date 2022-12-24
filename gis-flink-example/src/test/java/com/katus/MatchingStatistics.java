package com.katus;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-22
 */
@Slf4j
public class MatchingStatistics {
    public static void main(String[] args) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        LineIterator it = fsManipulator.getLineIterator("F:\\data\\graduation\\globalMR\\1119.csv");
        it.next();
        Map<Long, MRLine> map = new LinkedHashMap<>();
        while (it.hasNext()) {
            MRLine mrLine = new MRLine(it.next());
            map.put(mrLine.getId(), mrLine);
        }
        String[] paths = new String[]{
                "F:\\data\\graduation\\matching\\closest-matching",
                "F:\\data\\graduation\\matching\\closest-direction-matching",
                "F:\\data\\graduation\\matching\\closest-direction-accurate-matching",
                "F:\\data\\graduation\\matching\\closest-path-matching",
                "F:\\data\\graduation\\matching\\incremental-hidden-markov-matching",
                "F:\\data\\graduation\\matching\\present-hidden-markov-matching",
                "F:\\data\\graduation\\matching\\auto-fix-hidden-markov-matching\\Q2",
                "F:\\data\\graduation\\matching\\auto-fix-hidden-markov-matching\\Q5",
                "F:\\data\\graduation\\matching\\auto-fix-hidden-markov-matching\\Q10",
                "F:\\data\\graduation\\matching\\auto-fix-hidden-markov-matching\\Q20"
        };
        int index = 0;
        for (String path : paths) {
            String[] filenames = fsManipulator.list(path);
            for (String filename : filenames) {
                it = fsManipulator.getLineIterator(filename);
                while (it.hasNext()) {
                    String[] items = it.next().split(",");
                    long id = Long.parseLong(items[0]);
                    double x = Double.parseDouble(items[12]);
                    double y = Double.parseDouble(items[13]);
                    long edgeId = Long.parseLong(items[9]);
                    MRLine mrLine = map.get(id);
                    if (edgeId == mrLine.getEdgeId()) {
                        mrLine.getStatus()[index] = 2;   // 完全匹配正确
                    } else if (Math.abs(x - mrLine.x) + Math.abs(y - mrLine.y) <= 0.001) {
                        mrLine.getStatus()[index] = 1;   // 点匹配正确
                    } else {
                        mrLine.getStatus()[index] = 3;   // 匹配错误
                    }
                }
            }
            index++;
        }
        int[][] counts = new int[paths.length][4];
        double[][] accurate = new double[paths.length][4];
        for (Map.Entry<Long, MRLine> entry : map.entrySet()) {
            int[] status = entry.getValue().getStatus();
            for (int i = 0; i < status.length; i++) {
                counts[i][status[i]]++;
            }
        }
        for (int i = 0; i < counts.length; i++) {
            for (int j = 0; j < 4; j++) {
                accurate[i][j] = 1.0 * counts[i][j] / map.size();
            }
        }
        for (int i = 0; i < counts.length; i++) {
            log.info("{}", Arrays.toString(counts[i]));
            log.info("{}", Arrays.toString(accurate[i]));
        }
    }

    @Data
    public static class MRLine {
        private long id;
        private double x;
        private double y;
        private long edgeId;
        private int[] status;   // cm, cdm, cdam, cpm, ihmm, phmm, afhmm2, afhmm5, afhmm10, afhmm20;

        public MRLine() {
            this.status = new int[10];
        }

        public MRLine(String line) {
            String[] items = line.split(",");
            this.id = Long.parseLong(items[0]);
            this.x = Double.parseDouble(items[12]);
            this.y = Double.parseDouble(items[13]);
            this.edgeId = Long.parseLong(items[9]);
            this.status = new int[10];
        }
    }
}
