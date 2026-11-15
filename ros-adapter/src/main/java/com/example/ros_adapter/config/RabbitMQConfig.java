package com.example.ros_adapter.config;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String ROS_ADAPTER_QUEUE = "ros.adapter.queue";
    public static final String DRIVER_DISPATCH_QUEUE = "driver.dispatch.queue"; // <-- ADD THIS

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public Queue rosAdapterQueue() {
        return new Queue(ROS_ADAPTER_QUEUE, true);
    }

    @Bean
    public Queue driverDispatchQueue() {
        return new Queue(DRIVER_DISPATCH_QUEUE, true);
    }
}