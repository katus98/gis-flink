package cn.edu.zju.gis.td.example.experiment.entity;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * GPS点序列化与反序列化模式
 *
 * @author SUN Katus
 * @version 1.0, 2022-12-24
 */
public class GpsPointSerSchema implements DeserializationSchema<SerializedData.GpsPointSer>, SerializationSchema<SerializedData.GpsPointSer> {
    @Override
    public SerializedData.GpsPointSer deserialize(byte[] bytes) throws IOException {
        return SerializedData.GpsPointSer.parseFrom(bytes);
    }

    @Override
    public boolean isEndOfStream(SerializedData.GpsPointSer gpsPointSer) {
        return false;
    }

    @Override
    public byte[] serialize(SerializedData.GpsPointSer gpsPointSer) {
        return gpsPointSer.toByteArray();
    }

    @Override
    public TypeInformation<SerializedData.GpsPointSer> getProducedType() {
        return TypeInformation.of(SerializedData.GpsPointSer.class);
    }
}
