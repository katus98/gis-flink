package cn.edu.zju.gis.td.example.experiment.data;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import cn.edu.zju.gis.td.example.experiment.entity.SerializedData;
import cn.edu.zju.gis.td.example.experiment.entity.TextSerializable;
import cn.edu.zju.gis.td.example.experiment.global.GlobalConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-16
 */
@Slf4j
public class DataStreamGenerator {
    public static void main(String[] args) throws IOException, ParseException {
        // 生成编号1119的出租车GPS流
        generateStream(getBaseKafkaProps(),
                GlobalConfig.KAFKA_GPS_TEST_TOPIC,
                0,
                "D:\\data\\graduation\\gps\\1119.csv",
                line -> new GpsPoint(line).toSer());
        // 生成全量出租车GPS流
        generateStream(getBaseKafkaProps(),
                GlobalConfig.KAFKA_GPS_TOPIC,
                0,
                "",
                line -> new GpsPoint(line).toSer());
        // 生成全量交通事故流
        generateStream(getBaseKafkaProps(),
                GlobalConfig.KAFKA_ACCIDENT_TOPIC,
                0,
                "",
                line -> {
                    String[] items = line.split(",");
                    SerializedData.AccidentPointSer ser;
                    try {
                        ser = SerializedData.AccidentPointSer.newBuilder()
                                .setId(Long.parseLong(items[0]))
                                .setTimestamp(Long.parseLong(items[1]))
                                .setAddress(items[4])
                                .setInjuredNumber(Integer.parseInt(items[10]))
                                .setDeathNumber(Integer.parseInt(items[11]))
                                .setDeathLaterNumber(Integer.parseInt(items[12]))
                                .setMissingNumber(Integer.parseInt(items[13]))
                                .setInjuredNumber7(Integer.parseInt(items[14]))
                                .setDeathNumber7(Integer.parseInt(items[15]))
                                .setLon(Double.parseDouble(items[25]))
                                .setLat(Double.parseDouble(items[26]))
                                .setComprehension(Integer.parseInt(items[27]))
                                .build();
                    } catch (Exception e) {
                        ser = null;
                        log.warn("ACCIDENT LINE: {} CONTAINS ERROR!", line);
                    }
                    return ser;
                });
        // 生成全量交通违法流
        generateStream(getBaseKafkaProps(),
                GlobalConfig.KAFKA_ILLEGALITY_TOPIC,
                0,
                "",
                line -> {
                    String[] items = line.split(",");
                    SerializedData.IllegalityPointSer ser;
                    try {
                        ser = SerializedData.IllegalityPointSer.newBuilder()
                                .setId(Long.parseLong(items[1]))
                                .setTimestamp(Long.parseLong(items[2]))
                                .setSite(items[15])
                                .setAddress(items[19])
                                .setType(items[23])
                                .setLon(Double.parseDouble(items[28]))
                                .setLat(Double.parseDouble(items[29]))
                                .setComprehension(Integer.parseInt(items[30]))
                                .build();
                    } catch (Exception e) {
                        ser = null;
                        log.warn("ILLEGALITY LINE: {} CONTAINS ERROR!", line);
                    }
                    return ser;
                });
    }

    public static void generateStream(Properties properties, String topic, int partition, String filename, LoadFromTextFunction loadFunction) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        LineIterator it = fsManipulator.getLineIterator(filename);
        KafkaProducer<Long, byte[]> producer = new KafkaProducer<>(properties);
        long count = 0;
        while (it.hasNext()) {
            TextSerializable ser = loadFunction.load(it.next());
            ProducerRecord<Long, byte[]> record = new ProducerRecord<>(topic, partition, ser.getTimestamp(), ser.getId(), ser.toByteArray());
            Future<RecordMetadata> future = producer.send(record);
            try {
                RecordMetadata metadata = future.get();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            count++;
            if (count % 1000 == 0) {
                log.info("{} lines finished!", count);
            }
        }
        producer.flush();
        producer.close();
        log.info("All finished!");
    }

    private static Properties getBaseKafkaProps() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", GlobalConfig.KAFKA_SERVER);
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return properties;
    }
}
