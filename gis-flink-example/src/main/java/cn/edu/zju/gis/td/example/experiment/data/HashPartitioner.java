package cn.edu.zju.gis.td.example.experiment.data;

import cn.edu.zju.gis.td.example.experiment.entity.TextSerializable;

/**
 * 哈希分区器
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-28
 */
public class HashPartitioner implements SerPartitioner {
    private final int partitionCount;

    public HashPartitioner(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    @Override
    public int getPartition(TextSerializable ser) {
        return Math.abs(ser.hashCode()) % partitionCount;
    }
}
