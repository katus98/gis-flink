package com.katus;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-12
 */
@Slf4j
public class AnalysisUnitProcessing {
    public static void main(String[] args) throws IOException, SQLException {
//        log.info("{}", check("E:\\data\\graduation\\roads_ori\\roads_seg_f.csv"));
//        generateWKT("E:\\data\\graduation\\roads_ori\\roads_seg_f.csv", "E:\\data\\graduation\\roads_ori\\wkt_seg_f.csv");
        updateAllLineGeom("E:\\data\\graduation\\roads_ori\\wkt_seg_f.csv");
    }

    /**
     * 检查是否全部都是单体线段
     */
    private static boolean check(String filename) throws IOException {
        boolean res = true;
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        LineIterator it = fsManipulator.getLineIterator(filename);
        it.next();
        while (it.hasNext()) {
            String line = it.next();
            if (line.split("\t")[0].substring(19).contains("(")) {
                log.debug("[{}] IS MULTI.", line);
                res = false;
            }
        }
        return res;
    }

    private static void generateWKT(String input, String output) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        LineIterator it = fsManipulator.getLineIterator(input);
        it.next();
        List<String> content = new ArrayList<>();
        while (it.hasNext()) {
            String[] items = it.next().split("\t");
            String cor = items[0].substring(19, items[0].length() - 3);
            content.add(String.format("%s\tLINESTRING (%s)", items[1], cor));
        }
        fsManipulator.writeTextToFile(output, content);
    }

    private static void updateAllLineGeom(String filename) throws SQLException, IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        LineIterator it = fsManipulator.getLineIterator(filename);
        while (it.hasNext()) {
            updateLineGeom(it.next());
        }
    }

    private static void updateLineGeom(String line) throws SQLException {
        String sql = "UPDATE analysis_units SET roads_geom = ST_GeomFromText(?, 32650) WHERE id = ?";
        String[] items = line.split("\t");
        Connection conn = GlobalConfig.PG_ANA_SOURCE.getConnection();
        PreparedStatement preStmt = conn.prepareStatement(sql);
        preStmt.setString(1, items[1]);
        preStmt.setLong(2, Long.parseLong(items[0]));
        preStmt.executeUpdate();
        preStmt.close();
        conn.close();
    }
}
