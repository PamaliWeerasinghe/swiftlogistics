package com.example.wms_adapter.config;

import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.amqp.core.Queue;

@Configuration
public class RabbitMQConfig {
        public static final String WMS_ADAPTER_QUEUE = "wms.adapter.queue";

        @Bean
        public MessageConverter jsonMessageConverter() {
            return new JacksonJsonMessageConverter();
        }

//        @Bean
//        public Queue wmsAdapterQueue() {
//            return new Queue(WMS_ADAPTER_QUEUE, true); // durable queue
//        }

        @Bean
        public Queue rosAdapterQueue() {
            return new Queue(ROS_ADAPTER_QUEUE, true);
        }

}
