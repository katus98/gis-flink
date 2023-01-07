package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;

/**
 * 图计算器中的途经信息
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-07
 */
@Getter
public class StopInfo {
    /**
     * 位置ID
     */
    private final long id;
    /**
     * 到达位置需要的成本
     */
    private final double cost;

    public StopInfo(long id, double cost) {
        this.id = id;
        this.cost = cost;
    }
}
