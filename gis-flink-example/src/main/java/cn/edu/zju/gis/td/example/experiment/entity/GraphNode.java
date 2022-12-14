package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 1.0, 2022-12-11
 */
@Getter
@Setter
public class GraphNode {
    private long id;
    private boolean visited;
    private double cumulativeCost;
    private long previousNodeId;

    public GraphNode(long id) {
        this.id = id;
        this.visited = false;
        this.cumulativeCost = 1.0 * Integer.MAX_VALUE;
        this.previousNodeId = -1;
    }
}