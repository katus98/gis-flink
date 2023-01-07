package com.katus;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import cn.edu.zju.gis.td.example.experiment.global.ModelConstants;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-17
 */
@Slf4j
public class GpsProcessing {
    public static void main(String[] args) throws IOException, ParseException {
        process(new String[]{
                "F:\\data\\graduation\\gps_ori\\MDTUpInfo_0501.csv",
                "F:\\data\\graduation\\gps_ori\\MDTUpInfo_0502.csv",
                "F:\\data\\graduation\\gps_ori\\MDTUpInfo_0503.csv",
                "F:\\data\\graduation\\gps_ori\\MDTUpInfo_0504.csv"
        }, "F:\\data\\graduation\\gpsFilter\\", true);
    }

    private static void process(String[] files, String dir, boolean filter) throws IOException, ParseException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        Map<Integer, List<GpsPoint>> dataMap = new HashMap<>();
        for (String file : files) {
            LineIterator it = fsManipulator.getLineIterator(file);
            while (it.hasNext()) {
                GpsPoint gpsPoint = GpsPoint.loadByOri(it.next());
                long t = gpsPoint.getTimestamp();
                if (t >= GlobalConfig.TIME_0501 && t < GlobalConfig.TIME_0504) {
                    if (!dataMap.containsKey(gpsPoint.getTaxiId())) {
                        dataMap.put(gpsPoint.getTaxiId(), new ArrayList<>());
                    }
                    dataMap.get(gpsPoint.getTaxiId()).add(gpsPoint);
                }
            }
            log.info("File: '{}' finished!", file);
        }
        for (Map.Entry<Integer, List<GpsPoint>> entry : dataMap.entrySet()) {
            int taxiId = entry.getKey();
            List<GpsPoint> list = entry.getValue();
            list.sort(Comparator.comparingLong(GpsPoint::getTimestamp));
            List<String> content = new ArrayList<>();
            GpsPoint previousGps = list.get(0);
            content.add(previousGps.toLine());
            for (int i = 1; i < list.size(); i++) {
                GpsPoint gpsPoint = list.get(i);
                if (filter) {
                    if (gpsPoint.usefulValueEquals(previousGps) || (gpsPoint.posEquals(previousGps) && gpsPoint.getTimestamp() - previousGps.getTimestamp() > ModelConstants.MAX_FILTER_DELTA_TIME)) {
                        continue;
                    }
                    previousGps = gpsPoint;
                }
                content.add(gpsPoint.toLine());
            }
            fsManipulator.writeTextToFile(dir + taxiId + ".csv", content);
            log.info("Taxi Id: {} finished, valid: {}, delete: {}", taxiId, content.size(), list.size() - content.size());
        }
    }
}
