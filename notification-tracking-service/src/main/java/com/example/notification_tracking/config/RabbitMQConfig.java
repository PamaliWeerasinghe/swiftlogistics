package com.example.notification_tracking.config;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String ORDER_TRACKING_QUEUE = "order.tracking.queue";
    public static final String ORDER_DLQ = "order.dlq";
    public static final String ORDER_COMPENSATION_QUEUE = "order.compensation.queue";

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public Queue orderTrackingQueue() {
        return QueueBuilder.durable(ORDER_TRACKING_QUEUE)
                .withArgument("x-queue-mode", "lazy")
                .build();
    }

    @Bean
    public Queue orderDlq() {
        return QueueBuilder.durable(ORDER_DLQ).build();
    }

    @Bean
    public Queue orderCompensationQueue() {
        return QueueBuilder.durable(ORDER_COMPENSATION_QUEUE).build();
    }
}