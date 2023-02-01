package cn.edu.zju.gis.td.example.experiment.entity.schema;

import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * 交通事故点序列化与反序列化模式
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-31
 */
public class AccidentPointSerSchema implements DeserializationSchema<SerializedData.AccidentPointSer>, SerializationSchema<SerializedData.AccidentPointSer> {
    @Override
    public SerializedData.AccidentPointSer deserialize(byte[] bytes) throws IOException {
        return SerializedData.AccidentPointSer.parseFrom(bytes);
    }

    @Override
    public boolean isEndOfStream(SerializedData.AccidentPointSer ser) {
        return false;
    }

    @Override
    public byte[] serialize(SerializedData.AccidentPointSer ser) {
        return ser.toByteArray();
    }

    @Override
    public TypeInformation<SerializedData.AccidentPointSer> getProducedType() {
        return TypeInformation.of(SerializedData.AccidentPointSer.class);
    }
}
