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
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

/**
 * 流数据产生器
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-16
 */
@Slf4j
public class DataStreamGenerator {
    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws IOException, ParseException {
        int index = Integer.parseInt(args[0]);
        String filename = args[1];
        switch (index) {
            case 0:
                // 生成编号1119的出租车GPS流
                generateStream(getBaseKafkaProps(),
                        GlobalConfig.KAFKA_GPS_TEST_TOPIC,
                        new SinglePartitioner(),
                        filename,
                        line -> new GpsPoint(line).toSer());
                break;
            case 1:
                // 生成全量出租车GPS流
                generateStream(getBaseKafkaProps(),
                        GlobalConfig.KAFKA_GPS_TOPIC,
                        new GpsPartitioner(GlobalConfig.KAFKA_PARTITION_COUNT),
                        filename,
                        line -> new GpsPoint(line).toSer());
                break;
            case 2:
                // 生成全量交通事故流
                generateStream(getBaseKafkaProps(),
                        GlobalConfig.KAFKA_ACCIDENT_TOPIC,
                        new HashPartitioner(GlobalConfig.KAFKA_PARTITION_COUNT),
                        filename,
                        new LoadFromTextFunction() {
                            private final Pattern pattern = Pattern.compile("^[0-9]*$");
                            private long lastId = 400000000000000L;

                            @Override
                            public TextSerializable load(String line) {
                                String[] items = line.split(",");
                                SerializedData.AccidentPointSer ser;
                                try {
                                    long id;
                                    if (pattern.matcher(items[0]).matches()) {
                                        id = Long.parseLong(items[0]);
                                    } else {
                                        id = lastId++;
                                    }
                                    int inNum;
                                    if (items[10].isEmpty()) {
                                        inNum = 0;
                                    } else {
                                        inNum = Double.valueOf(items[10]).intValue();
                                    }
                                    ser = SerializedData.AccidentPointSer.newBuilder()
                                            .setId(id)
                                            .setTimestamp(FORMAT.parse(items[1]).getTime())
                                            .setAddress(items[4])
                                            .setInjuredNumber(inNum)
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
                                    log.warn("ACCIDENT LINE: {} CONTAINS ERROR!", line, e);
                                }
                                return ser;
                            }
                        });
                break;
            default:
                // 生成全量交通违法流
                generateStream(getBaseKafkaProps(),
                        GlobalConfig.KAFKA_ILLEGALITY_TOPIC,
                        new HashPartitioner(GlobalConfig.KAFKA_PARTITION_COUNT),
                        filename,
                        line -> {
                            String[] items = line.split(",");
                            SerializedData.IllegalityPointSer ser;
                            try {
                                ser = SerializedData.IllegalityPointSer.newBuilder()
                                        .setId(Long.parseLong(items[1].substring(5)))
                                        .setTimestamp(FORMAT.parse(items[2]).getTime())
                                        .setSite(items[15])
                                        .setAddress(items[19])
                                        .setType(items[23])
                                        .setLon(Double.parseDouble(items[28]))
                                        .setLat(Double.parseDouble(items[29]))
                                        .setComprehension(Integer.parseInt(items[30]))
                                        .build();
                            } catch (Exception e) {
                                ser = null;
                                log.warn("ILLEGALITY LINE: {} CONTAINS ERROR!", line, e);
                            }
                            return ser;
                        });
        }
    }

    public static void generateStream(Properties properties, String topic, SerPartitioner partitioner, String filename, LoadFromTextFunction loadFunction) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        LineIterator it = fsManipulator.getLineIterator(filename);
        KafkaProducer<Long, byte[]> producer = new KafkaProducer<>(properties);
        long count = 0;
        while (it.hasNext()) {
            TextSerializable ser = loadFunction.load(it.next());
            if (ser == null) continue;
            ProducerRecord<Long, byte[]> record = new ProducerRecord<>(topic, partitioner.getPartition(ser), ser.getTimestamp(), ser.getId(), ser.toByteArray());
            Future<RecordMetadata> future = producer.send(record);
            try {
                RecordMetadata metadata = future.get();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            count++;
            if (count % 10000 == 0) {
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
        // 确认级别 单条确认
        properties.put("acks", "1");
        // 重试次数 无限
        properties.put("retries", 0);
        // 批量大小 512K
        properties.put("batch.size", 512 * 1024);
        // 延迟发送时间 500ms
//        properties.put("linger.ms", 500);
        // 缓冲区大小 1024M
        properties.put("buffer.memory", 1024 * 1024 * 1024);
        // key 序列化方法
        properties.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        // value 序列化方法
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return properties;
    }
}
