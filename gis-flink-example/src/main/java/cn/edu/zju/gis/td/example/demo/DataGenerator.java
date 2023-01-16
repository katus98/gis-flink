package cn.edu.zju.gis.td.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author SUN Katus
 * @version 1.0, 2022-11-08
 */
@Slf4j
public class DataGenerator {
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
