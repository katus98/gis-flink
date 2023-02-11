package cn.edu.zju.gis.td.example.experiment.entity.schema;

import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * 匹配点序列化与反序列化模式
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-24
 */
@Slf4j
public class MatPointSerSchema implements DeserializationSchema<SerializedData.MatPointSer>, SerializationSchema<SerializedData.MatPointSer> {
    @Override
    public SerializedData.MatPointSer deserialize(byte[] bytes) throws IOException {
        return SerializedData.MatPointSer.parseFrom(bytes);
    }

    @Override
    public boolean isEndOfStream(SerializedData.MatPointSer matPointSer) {
        return false;
    }

    @Override
    public byte[] serialize(SerializedData.MatPointSer matPointSer) {
        return matPointSer.toByteArray();
    }

    @Override
    public TypeInformation<SerializedData.MatPointSer> getProducedType() {
        return TypeInformation.of(SerializedData.MatPointSer.class);
    }
}
