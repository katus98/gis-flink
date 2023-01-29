package cn.edu.zju.gis.td.example.experiment.data;

import cn.edu.zju.gis.td.example.experiment.entity.TextSerializable;

/**
 * 单分区器
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-29
 */
public class SinglePartitioner implements SerPartitioner {
    @Override
    public int getPartition(TextSerializable ser) {
        return 0;
    }
}
