package com.example.wms_adapter.listener;

import com.example.wms_adapter.dto.OrderDTO;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;

@Component
public class WmsAdapterListener {

    private static final String WMS_ADAPTER_QUEUE = "wms.adapter.queue";
    // Mock Warehouse Endpoint URL
    private static final String WMS_API_URL = "http://localhost:8081/mock-wms/api/dispatch";

    private final WebClient webClient;

    @Autowired
    public WmsAdapterListener() {
        this.webClient = WebClient.builder().build();
    }

    @RabbitListener(queues = WMS_ADAPTER_QUEUE)
    public void processWarehouseOrder(OrderDTO orderDTO, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.out.println("\n📦 [WMS ADAPTER] Picked up Order: " + orderDTO.getOrderId() + " from Queue.");

        try {
            System.out.println("📦 [WMS ADAPTER] Translating DTO to Warehouse Dispatch Payload...");

            // Transmit request to WMS system via HTTP POST
            String wmsResponse = this.webClient.post()
                    .uri(WMS_API_URL)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(orderDTO)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block(); // Intentionally block to ensure network confirmation before acknowledging message

            System.out.println("✔ [WMS ADAPTER] Warehouse System Acknowledged Fulfillment: " + wmsResponse);

            // Acknowledge RabbitMQ that message has been fully processed
            channel.basicAck(tag, false);
            System.out.println("🎉 [WMS ADAPTER] Message ACK sent. Pipeline execution completed successfully!");

        } catch (Exception e) {
            System.out.println("❌ [WMS ADAPTER] Failed to dispatch to Warehouse: " + e.getMessage());
            // Re-queue the message to retry when WMS system recovers
            channel.basicNack(tag, false, true);
        }
    }
}