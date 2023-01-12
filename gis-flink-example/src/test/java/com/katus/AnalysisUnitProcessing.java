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

    /**
     * 将QGIS生成的CSV数据转化为标准格式
     */
    private static void generateWKT(String input, String output) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        LineIterator it = fsManipulator.getLineIterator(input);
        it.next();
        List<String> content = new ArrayList<>();
        while (it.hasNext()) {
            String[] items = it.next().split("\t");
            String cor = items[0].substring(19, items[0].length() - 3);
            content.add(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%b\t%b\t%s\tLINESTRING (%s)",
                    items[1], items[17], items[2], items[4], items[5], items[6], items[7], items[8],
                    items[9], items[10], items[11].equals("T"), items[12].equals("T"), items[13], cor));
        }
        fsManipulator.writeTextToFile(output, content);
    }

    /**
     * 根据标准分析单元数据更新数据库信息
     */
    private static void updateAllLineGeom(String filename) throws SQLException, IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        LineIterator it = fsManipulator.getLineIterator(filename);
        while (it.hasNext()) {
            updateLineGeom(it.next());
        }
    }

    private static void updateLineGeom(String line) throws SQLException {
        String sql = "UPDATE analysis_units SET " +
                "length = ? " +
                "osm_id = ? " +
                "fclass = ? " +
                "ori_name = ? " +
                "ori_ref = ? " +
                "new_name = ? " +
                "new_ref = ? " +
                "oneway = ? " +
                "maxspeed = ? " +
                "is_bridge = ? " +
                "is_tunnel = ? " +
                "hierarchy = ? " +
                "roads_geom = ST_GeomFromText(?, 32650) " +
                "WHERE id = ?";
        String[] items = line.split("\t");
        Connection conn = GlobalConfig.PG_ANA_SOURCE.getConnection();
        PreparedStatement preStmt = conn.prepareStatement(sql);
        preStmt.setDouble(1, Double.parseDouble(items[1]));
        preStmt.setInt(2, Integer.parseInt(items[2]));
        preStmt.setString(3, items[3]);
        preStmt.setString(4, items[4]);
        preStmt.setString(5, items[5]);
        preStmt.setString(6, items[6]);
        preStmt.setString(7, items[7]);
        preStmt.setString(8, items[8]);
        preStmt.setDouble(9, Double.parseDouble(items[9]));
        preStmt.setBoolean(10, Boolean.parseBoolean(items[10]));
        preStmt.setBoolean(11, Boolean.parseBoolean(items[11]));
        preStmt.setInt(12, Integer.parseInt(items[12]));
        preStmt.setString(13, items[13]);
        preStmt.setLong(14, Long.parseLong(items[0]));
        preStmt.executeUpdate();
        preStmt.close();
        conn.close();
    }
}
