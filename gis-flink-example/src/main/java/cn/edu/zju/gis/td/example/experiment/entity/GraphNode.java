package cn.edu.zju.gis.td.example.experiment.entity;

import cn.edu.zju.gis.td.example.experiment.matching.MatchingConstants;
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
    private long presentEdgeId;

    public GraphNode(long id) {
        this.id = id;
        this.visited = false;
        this.cumulativeCost = MatchingConstants.MAX_COST;
        this.previousNodeId = -1;
        this.presentEdgeId = -1;
    }
}
