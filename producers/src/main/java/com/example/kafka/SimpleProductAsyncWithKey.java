package com.example.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProductAsyncWithKey {
    private static final Logger logger = LoggerFactory.getLogger(SimpleProductAsyncWithKey.class.getName());
    private static final String TOPIC_NAME = "multipart-topic";

    public static void main(String[] args) {
        //bootstrap.servers, key.serializer, value.serializer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer 객체 생성
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int seq = 0; seq < 20; seq++) {
            //ProducerRecord 객체 생성
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, seq,
                    "HELLO WORLD" + seq);

            // 메시지 전송
            Callback callback = new CustomCallback(seq);
            kafkaProducer.send(producerRecord, callback);
        }

        kafkaProducer.close();
    }

    private static class CustomCallback implements Callback {
        private int seq;

        public CustomCallback(int seq) {
            this.seq = seq;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception e) {
            if (e == null) {
                logger.info("seq: {} partition: {} offset: {}", this.seq, metadata.partition(), metadata.offset());
                return;
            }

            logger.error("exception from broker " + e.getMessage());
        }
    }
}
