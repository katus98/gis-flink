package cn.edu.zju.gis.td.example.experiment.data;

import cn.edu.zju.gis.td.example.experiment.entity.TextSerializable;

/**
 * Kafka序列对象分区器接口
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-28
 */
public interface SerPartitioner {
    int getPartition(TextSerializable ser);
}
