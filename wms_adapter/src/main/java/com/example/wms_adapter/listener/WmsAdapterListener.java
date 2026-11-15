package com.example.wms_adapter.listener;

import com.example.wms_adapter.config.RabbitMQConfig;
import com.example.wms_adapter.dto.OrderDTO;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.util.Map;

@Component
public class WmsAdapterListener {

    private static final String WMS_ADAPTER_QUEUE = "wms.adapter.queue";
    private static final String WMS_API_URL = "http://localhost:8081/mock-wms/api/dispatch";

    @Autowired
    private RabbitTemplate rabbitTemplate;

    private final WebClient webClient;

    public WmsAdapterListener() {
        this.webClient = WebClient.builder().build();
    }

    @RabbitListener(queues = WMS_ADAPTER_QUEUE)
    public void processWarehouseOrder(OrderDTO orderDTO, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.out.println("\n📦 [WMS ADAPTER] Picked up Order: " + orderDTO.getOrderId() + " from Queue.");

        Map<String, Object> warehouseRequest = Map.of(
                "orderId", orderDTO.getOrderId(),
                "clientId", orderDTO.getClientId(),
                "address", orderDTO.getAddress(),
                "itemCount", 1
        );

        try {
            System.out.println("🔌 [WMS ADAPTER] Sending dispatch to mock WMS API at " + WMS_API_URL);

            Map<String, Object> response = webClient.post()
                    .uri(WMS_API_URL)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(warehouseRequest)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block();

            System.out.println("✔ [WMS ADAPTER] Mock warehouse accepted dispatch: " + response);

            rabbitTemplate.convertAndSend(RabbitMQConfig.ROS_ADAPTER_QUEUE, orderDTO);
            System.out.println("➡️ [WMS ADAPTER] Forwarded Order " + orderDTO.getOrderId() + " to ros.adapter.queue");

            channel.basicAck(tag, false);
            System.out.println("🎉 [WMS ADAPTER] RabbitMQ ACK sent. Handed off to ROS stage!");
        } catch (Exception e) {
            System.out.println("❌ [WMS ADAPTER] WMS HTTP call failed: " + e.getMessage());
            channel.basicNack(tag, false, true);
        }
    }
}