package com.practice.kafka.producer;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.practice.kafka.model.Order;

public class OrderSerializer implements Serializer<Order> {
    private static final Logger logger = LoggerFactory.getLogger(OrderSerializer.class.getName());
    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public byte[] serialize(String topic, Order order) {
        byte[] serializedOrder = null;

        try {
            objectMapper.writeValueAsBytes(order);
        } catch (JsonProcessingException e) {
            logger.error("Json processing exception: " + e.getMessage());
        }
        return serializedOrder;
    }
}
