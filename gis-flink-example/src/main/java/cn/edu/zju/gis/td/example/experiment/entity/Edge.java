package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-07
 */
@Getter
@Setter
@ToString
public class Edge {
    private long id;
    private long startId;
    private long endId;

    public Edge() {
    }

    public Edge(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.startId = rs.getLong("start_id");
        this.endId = rs.getLong("end_id");
    }
}
