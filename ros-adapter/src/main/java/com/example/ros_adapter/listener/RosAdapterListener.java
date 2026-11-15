package com.example.ros_adapter.listener;

import com.example.ros_adapter.config.RabbitMQConfig;
import com.example.ros_adapter.dto.OrderDTO;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class RosAdapterListener {

    @Value("${ros.api.url}")
    private String rosApiUrl;

    @Value("${ros.api.key}")
    private String rosApiKey;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    private final WebClient webClient = WebClient.builder().build();

    @RabbitListener(queues = RabbitMQConfig.ROS_ADAPTER_QUEUE)
    public void handleRouteOptimization(OrderDTO orderDTO, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.out.println("\n🗺️ [ROS ADAPTER] Picked up Order: " + orderDTO.getOrderId());

        Map<String, Object> vendorRequest = Map.of(
                "orderId", orderDTO.getOrderId(),
                "clientId", orderDTO.getClientId(),
                "deliveryAddress", orderDTO.getAddress(),
                "originDepot", "SwiftLogistics Central Warehouse - Colombo",
                "availableVehicles", List.of("VAN-CMB-01", "VAN-CMB-04", "TRUCK-WP-02"),
                "optimizationGoal", "MINIMIZE_TRANSIT_TIME"
        );

        try {
            Map<String, Object> rosResponse = this.webClient.post()
                    .uri(rosApiUrl)
                    .header("X-API-Key", rosApiKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(vendorRequest)
                    .retrieve()
                    .bodyToMono(new ParameterizedTypeReference>() {})
                    .block();

            System.out.println("✔ [ROS ADAPTER] Cloud ROS Confirmed: " + rosResponse);

            // Build Enriched Driver Manifest Payload
            Map<String, Object> driverManifestPayload = new HashMap<>();
            driverManifestPayload.put("orderId", orderDTO.getOrderId());
            driverManifestPayload.put("clientId", orderDTO.getClientId());
            driverManifestPayload.put("address", orderDTO.getAddress());
            driverManifestPayload.put("routeId", rosResponse != null ? rosResponse.get("routeId") : "RT-DEFAULT");
            driverManifestPayload.put("assignedVehicle", rosResponse != null ? rosResponse.get("assignedVehicle") : "VAN-CMB-01");
            driverManifestPayload.put("estimatedDistanceKm", rosResponse != null ? rosResponse.get("estimatedDistanceKm") : 5.0);
            driverManifestPayload.put("estimatedEtaMinutes", rosResponse != null ? rosResponse.get("estimatedEtaMinutes") : 15);

            // Forward to Route & Driver Service Queue BEFORE Acking
            rabbitTemplate.convertAndSend(RabbitMQConfig.DRIVER_DISPATCH_QUEUE, driverManifestPayload);
            System.out.println("➡️ [ROS ADAPTER] Forwarded Manifest for " + orderDTO.getOrderId() + " to driver.dispatch.queue");

            channel.basicAck(tag, false);

        } catch (Exception e) {
            System.err.println("❌ [ROS ADAPTER] Cloud ROS call failed: " + e.getMessage());
            channel.basicNack(tag, false, true);
        }
    }
}