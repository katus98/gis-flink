package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-02
 */
@Getter
@Setter
public class FlowAndSpeed {
    private long timestamp;
    private long nodeId;
    private long flow;
    private double speed;

    public String toLine() {
        return timestamp + "," + nodeId + "," + flow + "," + speed;
    }

    @Override
    public String toString() {
        return toLine();
    }
}
