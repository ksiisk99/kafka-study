package com.practice.kafka.producer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.practice.kafka.model.Order;

public class OrderSerdeProducer {
    private static final Logger logger = LoggerFactory.getLogger(OrderSerdeProducer.class.getName());
    private static final String TOPIC_NAME = "order-serde-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, com.practice.kafka.producer.OrderSerializer.class.getName());

        //KafkaProducer 객체 생성
        KafkaProducer<String, Order> kafkaProducer = new KafkaProducer<>(properties);
        String filePath = "/Users/gimsang-in/Desktop/kafka-test/practice/src/main/resources/pizza_sample.txt";

        sendFileMessages(kafkaProducer, filePath);

        kafkaProducer.close();
    }

    private static void sendFileMessages(KafkaProducer<String, Order> kafkaProducer, String filePath) {

        String line = "";
        final String delimiter = ",";

        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            while ((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuilder value = new StringBuilder();

                Order order = new Order(tokens[1], tokens[2], tokens[3], tokens[4], tokens[5], tokens[6],
                        LocalDateTime.parse(tokens[7].trim(), formatter));

                sendMessage(kafkaProducer, key, order);
            }

        } catch (FileNotFoundException e) {
            logger.info(e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void sendMessage(KafkaProducer<String, Order> kafkaProducer, String key, Order value) {
        ProducerRecord<String, Order> producerRecord = new ProducerRecord<>(TOPIC_NAME, key, value);
        logger.info("key:{}, value:{}", key, value);

        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                logger.info("\n ###### record metadata received ##### \n"
                        + "partition: " + metadata.partition() + "\n"
                        + "offset: " + metadata.offset() + "\n"
                        + "timestamp: " + metadata.timestamp());
            } else {
                logger.error("exception error from broker " + exception.getMessage());
            }
        });
    }

}
