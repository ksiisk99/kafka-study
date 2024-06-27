package com.practice.kafka.consumer;

import java.io.IOException;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.practice.kafka.model.Order;

public class OrderDeserializer implements Deserializer<Order> {
    private static final Logger logger = LoggerFactory.getLogger(OrderDeserializer.class.getName());
    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public Order deserialize(String topic, byte[] data) {
        Order order = null;

        try {
            order = objectMapper.readValue(data, Order.class);
        } catch (IOException e) {
            logger.error("Object mapper deserialization error" + e.getMessage());
        }
        return order;
    }
}
