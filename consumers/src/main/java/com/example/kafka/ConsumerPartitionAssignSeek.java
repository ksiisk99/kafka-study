package com.example.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerPartitionAssignSeek {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerPartitionAssignSeek.class.getName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_pizza_assign_seek2");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        String topicName = "pizza-topic";

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        kafkaConsumer.assign(List.of(topicPartition)); // 파티션 지정
        kafkaConsumer.seek(topicPartition, 10L); // offset 지정하는데 offset이 지정한 수보다 작다면 카프카 offset수부터 읽음

        // main thread 종료시 별도의 thread로 KafkaConsumer wakeup()메소드를 호출하게 함.
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("main program start to exit by calling wakeup");

            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        pollCommitSync(kafkaConsumer);
        //pollCommitAsync(kafkaConsumer);
    }

    private static void pollCommitAsync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000L));
                logger.info(" ####### loopCnt: {} consumerRecords count: {}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key:{}, record value:{}, partition:{}",
                            record.key(), record.value(), record.partition());
                }

                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                        if (e != null) {
                            logger.error("offsets: {} is not completed, error: {}", offsets, e);
                        }
                    }
                });
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception ahs been called");
        } finally {
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }

    private static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000L));
                logger.info(" ####### loopCnt: {} consumerRecords count: {}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key:{}, record value:{}, partition:{}",
                            record.key(), record.value(), record.partition());
                }

                try {
                    kafkaConsumer.commitSync();
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception ahs been called");
        } finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }
}
