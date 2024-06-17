package com.example.kafka;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javafaker.Faker;

public class PizzaProducer {
    private static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class.getName());

    public static void main(String[] args) {
        //bootstrap.servers, key.serializer, value.serializer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer 객체 생성
        String topicName = "pizza-topic";
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        sendPizzaMessage(kafkaProducer, topicName, -1, 100, 100, 1000, true);

        kafkaProducer.close();
    }

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName, int iterCount, int interIntervalMillis, int intervalMillis, int intervalCount, boolean sync) {
        PizzaMessage pizzaMessage = new PizzaMessage();
        long seed = 2024;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        int iterSeq = 0;
        while (iterSeq++ != iterCount) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, pMessage.get("key"),
                    pMessage.get("message"));

            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            if((intervalCount > 0) && ((iterSeq % intervalCount)==0)) {
                try {
                    logger.info("####### IntervalCount:"+intervalCount+" intervalMillis:"+intervalMillis+" ##########");
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            if(interIntervalMillis > 0) {
                try {
                    logger.info("InterIntervalMillis: "+interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord, HashMap<String, String> pMessage, boolean sync) {
        if (!sync) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("async message: {} partition: {} offset: {}", pMessage.get("key"), metadata.partition(),
                            metadata.offset());
                    return;
                }

                logger.error("exception from broker " + exception.getMessage());
            });
            return;
        }

        try {
            RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
            logger.info("sync message: {} partition: {} offset: {}", pMessage.get("key"), metadata.partition(),
                    metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage());
        }

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
