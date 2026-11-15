package com.example.ros_adapter.listener;

import com.example.ros_adapter.config.RabbitMQConfig;
import com.example.ros_adapter.dto.OrderDTO;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Component
public class RosAdapterListener {

    @Value("${ros.api.url}")
    private String rosApiUrl;

    @Value("${ros.api.key}")
    private String rosApiKey;

    private final WebClient webClient;

    public RosAdapterListener() {
        this.webClient = WebClient.builder().build();
    }

    @RabbitListener(queues = RabbitMQConfig.ROS_ADAPTER_QUEUE)
    public void handleRouteOptimization(OrderDTO orderDTO, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.out.println("\n🗺️ [ROS ADAPTER] Picked up Order: " + orderDTO.getOrderId() + " from ros.adapter.queue");

        // Translate internal OrderDTO -> Cloud ROS Vendor REST Schema
        Map<String, Object> vendorRequest = Map.of(
                "orderId", orderDTO.getOrderId(),
                "clientId", orderDTO.getClientId(),
                "deliveryAddress", orderDTO.getAddress(),
                "originDepot", "SwiftLogistics Central Warehouse - Colombo",
                "availableVehicles", List.of("VAN-CMB-01", "VAN-CMB-04", "TRUCK-WP-02"),
                "optimizationGoal", "MINIMIZE_TRANSIT_TIME"
        );

        try {
            System.out.println("🗺️ [ROS ADAPTER] Sending REST POST to Cloud ROS Vendor (" + rosApiUrl + ")...");

            String response = this.webClient.post()
                    .uri(rosApiUrl)
                    .header("X-API-Key", rosApiKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(vendorRequest)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            System.out.println("✔ [ROS ADAPTER] Cloud ROS Optimized Route Confirmed: " + response);

            // Acknowledge RabbitMQ only after 200 OK from Cloud ROS
            channel.basicAck(tag, false);
            System.out.println("🏁 [ROS ADAPTER] Final ACK sent. End-to-End SwiftLogistics Pipeline Complete!");

        } catch (Exception e) {
            System.err.println("❌ [ROS ADAPTER] Failed to reach Cloud ROS API: " + e.getMessage());
            // Re-queue message so it stays safe in ros.adapter.queue if Cloud ROS is toggled offline
            channel.basicNack(tag, false, true);
        }
    }
}