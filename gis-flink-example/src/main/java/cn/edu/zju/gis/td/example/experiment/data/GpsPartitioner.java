package cn.edu.zju.gis.td.example.experiment.data;

import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import cn.edu.zju.gis.td.example.experiment.entity.TextSerializable;

/**
 * 出租车牌号分区器
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-28
 */
public class GpsPartitioner implements SerPartitioner {
    private final int partitionCount;

    public GpsPartitioner(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    @Override
    public int getPartition(TextSerializable ser) {
        return ((SerializedData.GpsPointSer) ser).getTaxiId() % partitionCount;
    }
}
