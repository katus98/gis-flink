package cn.edu.zju.gis.td.example.experiment.entity;

import cn.edu.zju.gis.td.example.experiment.global.GlobalUtil;
import lombok.Data;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-07
 */
@Data
public class MatchingResult {
    private Point matchingPoint;
    private EdgeWithInfo edgeWithInfo;
    private double dis;

    public MatchingResult(ResultSet rs) throws SQLException, ParseException {
        this.matchingPoint = (Point) GlobalUtil.WKT_READER.read(rs.getString("cp"));
        this.edgeWithInfo = new EdgeWithInfo(rs);
        this.dis = rs.getDouble("dis");
    }
}
