package cn.edu.zju.gis.td.example.experiment.data;

import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import cn.edu.zju.gis.td.example.experiment.entity.GpsPoint;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
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
 * @version 1.0, 2022-11-08
 */
@Slf4j
public class DataGenerator {
    public static void main(String[] args) throws IOException, ParseException {
        generateTaxiStream(getKafkaProps());
    }

    private static Properties getKafkaProps() {
        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "*:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

    private static void generateTaxiStream(Properties properties) throws IOException, ParseException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        LineIterator it = fsManipulator.getLineIterator("F:\\data\\graduation\\gps_ori\\MDTUpInfo_0501.csv");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ObjectMapper mapper = new ObjectMapper();
        long count = 0;
        while (it.hasNext()) {
            GpsPoint point = new GpsPoint(it.next());
            ProducerRecord<String, String> record = new ProducerRecord<>("taxi-test-0501", 0, point.getTimestamp(), String.valueOf(point.getId()), mapper.writeValueAsString(point));
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

    /**
     * 直接发送
     */
    private static void strSend(Properties properties) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test", "key-" + i, "value-" + i);
            producer.send(record);
        }
        producer.flush();
        producer.close();
    }

    /**
     * 同步发送
     */
    private static void synSend(Properties properties) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test", "key-" + i, "syn value-" + i);
            Future<RecordMetadata> future = producer.send(record);
            try {
                RecordMetadata metadata = future.get();
                log.info("partition: " + metadata.partition() + ", offset: " + metadata.offset());
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        producer.flush();
        producer.close();
    }

    /**
     * 异步发送
     */
    private static void aSynSend(Properties properties) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test", "key-" + i, "aSyn value-" + i);
            Future<RecordMetadata> future = producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    }
                    if (metadata != null) {
                        log.info("partition: " + metadata.partition() + ", offset: " + metadata.offset());
                    }
                }
            });
            try {
                RecordMetadata metadata = future.get();
                log.info(metadata.partition() + ", " + metadata.topic());
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.flush();
        producer.close();
    }
}
