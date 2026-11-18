package com.example.cms_adapter.config;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String CMS_ADAPTER_QUEUE = "cms.adapter.queue";
    public static final String WMS_ADAPTER_QUEUE = "wms.adapter.queue";
    public static final String ORDER_DLQ = "order.dlq";
    public static final String ORDER_COMPENSATION_QUEUE = "order.compensation.queue";

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new JacksonJsonMessageConverter();
    }

    @Bean
    public Queue cmsAdapterQueue() {
        return new Queue(CMS_ADAPTER_QUEUE, true);
    }

    @Bean
    public Queue wmsAdapterQueue() {
        return new Queue(WMS_ADAPTER_QUEUE, true);
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
