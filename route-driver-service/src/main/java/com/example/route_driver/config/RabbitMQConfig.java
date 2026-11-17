package com.example.route_driver.config;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String DRIVER_DISPATCH_QUEUE = "driver.dispatch.queue";
    public static final String ORDER_TRACKING_QUEUE = "order.tracking.queue";

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public Queue driverDispatchQueue() {
        return new Queue(DRIVER_DISPATCH_QUEUE, true);
    }

    @Bean
    public Queue orderTrackingQueue() {
        return QueueBuilder.durable(ORDER_TRACKING_QUEUE)
                .withArgument("x-queue-mode", "lazy")
                .build();
    }
}