package cn.edu.zju.gis.td.example.experiment.data;

import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import cn.edu.zju.gis.td.example.experiment.entity.TextSerializable;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * @author SUN Katus
 * @version 1.0, 2023-02-10
 */
public class MpsPartitioner extends FlinkKafkaPartitioner<SerializedData.MatPointSer> implements SerPartitioner {
    private final int partitionCount;

    public MpsPartitioner(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    @Override
    public int getPartition(TextSerializable ser) {
        return ((SerializedData.MatPointSer) ser).getTaxiId() % partitionCount;
    }

    @Override
    public int partition(SerializedData.MatPointSer record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        return partitions[record.getTaxiId() % partitions.length];
    }
}
