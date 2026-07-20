package com.example.cms_adapter.config;

import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.amqp.core.Queue;

@Configuration
public class RabbitMQConfig {

    public static final String CMS_ADAPTER_QUEUE = "cms.adapter.queue";
    public static final String WMS_ADAPTER_QUEUE = "wms.adapter.queue";

    // 1. JSON Converter (fixes serialization)
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new JacksonJsonMessageConverter();
    }

    // 2. EXPLICITLY DECLARE CMS QUEUE
    // Spring AMQP's RabbitAdmin will see this bean and automatically build
    // the queue inside RabbitMQ upon app startup!
    @Bean
    public Queue cmsAdapterQueue() {
        return new Queue(CMS_ADAPTER_QUEUE, true); // true = durable queue (persists on restart)
    }

    // 3. EXPLICITLY DECLARE WMS QUEUE (For your next step)
    @Bean
    public Queue wmsAdapterQueue() {
        return new Queue(WMS_ADAPTER_QUEUE, true);
    }

}
