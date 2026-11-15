package com.example.ros_adapter.config;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String ROS_ADAPTER_QUEUE = "ros.adapter.queue";

    // Ensures incoming JSON messages from wms-adapter map cleanly to local OrderDTO
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // Automatically creates ros.adapter.queue in RabbitMQ on startup
    @Bean
    public Queue rosAdapterQueue() {
        return new Queue(ROS_ADAPTER_QUEUE, true);
    }
}