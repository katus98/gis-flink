package cn.edu.zju.gis.td.example.experiment.entity.schema;

import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * 交通违法点序列化与反序列化模式
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-31
 */
public class IllegalityPointSerSchema implements DeserializationSchema<SerializedData.IllegalityPointSer>, SerializationSchema<SerializedData.IllegalityPointSer> {
    @Override
    public SerializedData.IllegalityPointSer deserialize(byte[] bytes) throws IOException {
        return SerializedData.IllegalityPointSer.parseFrom(bytes);
    }

    @Override
    public boolean isEndOfStream(SerializedData.IllegalityPointSer illegalityPointSer) {
        return false;
    }

    @Override
    public byte[] serialize(SerializedData.IllegalityPointSer illegalityPointSer) {
        return illegalityPointSer.toByteArray();
    }

    @Override
    public TypeInformation<SerializedData.IllegalityPointSer> getProducedType() {
        return TypeInformation.of(SerializedData.IllegalityPointSer.class);
    }
}
