package com.example.order_intake.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.amqp.core.Queue;

@Configuration
public class RabbitMQConfig {
    public static final String order_intake_queue = "order.intake.queue";

    @Bean
    public Queue orderIntakeQueue(){
        return new Queue(order_intake_queue, true);
    }

}
