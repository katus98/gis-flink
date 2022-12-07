package cn.edu.zju.gis.td.example.experiment.entity;

import cn.edu.zju.gis.td.example.experiment.global.GlobalUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.ParseException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-07
 */
@Getter
@Setter
@ToString
public class EdgeWithInfo extends Edge {
    private String osmId;
    private String fClass;
    private String oriName, oriRef, newName, newRef;
    private boolean isOneway;
    private double maxSpeed;
    private boolean isBridge, isTunnel;
    private int hierarchy;
    private double length;
    private List<Double> directions;
    private LineString geometry;

    public EdgeWithInfo() {
        this.directions = new ArrayList<>();
    }

    public EdgeWithInfo(ResultSet rs) throws SQLException, ParseException {
        super(rs);
        this.osmId = rs.getString("osm_id");
        this.fClass = rs.getString("fclass");
        this.oriName = rs.getString("ori_name");
        this.oriRef = rs.getString("ori_ref");
        this.newName = rs.getString("new_name");
        this.newRef = rs.getString("new_ref");
        this.isOneway = rs.getBoolean("is_oneway");
        this.maxSpeed = rs.getDouble("maxspeed");
        this.isBridge = rs.getBoolean("is_bridge");
        this.isTunnel = rs.getBoolean("is_tunnel");
        this.hierarchy = rs.getInt("hierarchy");
        this.length = rs.getDouble("length");
        this.directions = rs.getObject("directions", List.class);
        this.geometry = (LineString) GlobalUtil.WKT_READER.read(rs.getString("wkt"));
    }
}
