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
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RosAdapterListener {

    private static final ConcurrentHashMap<String, Integer> RETRY_COUNTS = new ConcurrentHashMap<>();

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

        Map<String, Object> driverManifestPayload = new HashMap<>();

        try {
            Map<String, Object> rosResponse = this.webClient.post()
                   .uri(rosApiUrl)
                   .header("X-API-Key", rosApiKey)
                   .contentType(MediaType.APPLICATION_JSON)
                   .bodyValue(vendorRequest)
                   .retrieve()
                   .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                   .block();

            System.out.println("✔ [ROS ADAPTER] Cloud ROS Confirmed: " + rosResponse);

            driverManifestPayload = new HashMap<>();
            driverManifestPayload.put("orderId", orderDTO.getOrderId());
            driverManifestPayload.put("clientId", orderDTO.getClientId());
            driverManifestPayload.put("address", orderDTO.getAddress());
            driverManifestPayload.put("routeId", rosResponse != null ? rosResponse.get("routeId") : "RT-DEFAULT");
            driverManifestPayload.put("assignedVehicle", rosResponse != null ? rosResponse.get("assignedVehicle") : "VAN-CMB-01");
            driverManifestPayload.put("estimatedDistanceKm", rosResponse != null ? rosResponse.get("estimatedDistanceKm") : 5.0);
            driverManifestPayload.put("estimatedEtaMinutes", rosResponse != null ? rosResponse.get("estimatedEtaMinutes") : 15);

            rabbitTemplate.convertAndSend(RabbitMQConfig.DRIVER_DISPATCH_QUEUE, driverManifestPayload);
            System.out.println("➡️ [ROS ADAPTER] Forwarded Manifest for " + orderDTO.getOrderId() + " to driver.dispatch.queue");

            rabbitTemplate.convertAndSend("order.tracking.queue", Map.of(
                   "orderId", orderDTO.getOrderId(),
                   "clientId", orderDTO.getClientId(),
                   "stage", "ROUTE_OPTIMIZED",
                   "details", "Assigned Vehicle: " + driverManifestPayload.get("assignedVehicle")
            ));

            RETRY_COUNTS.remove(orderDTO.getOrderId());
            channel.basicAck(tag, false);

        } catch (Exception e) {
            String orderId = orderDTO.getOrderId();
            int attempts = RETRY_COUNTS.merge(orderId, 1, Integer::sum);
            String reason = "ROS optimization call failed: " + e.getMessage();

            if (attempts <= 3) {
                System.out.println("⚠️ [ROS ADAPTER] Retry " + attempts + " for order " + orderId + " after temporary failure.");
                try {
                   Thread.sleep(1500);
                } catch (InterruptedException interruptedException) {
                   Thread.currentThread().interrupt();
                }
                channel.basicNack(tag, false, true);
                return;
            }

            publishCompensation(orderDTO, "ROS", reason);
            RETRY_COUNTS.remove(orderId);
            channel.basicAck(tag, false);
        }
    }

    private void publishCompensation(OrderDTO orderDTO, String failedStage, String reason) {
        Map<String, Object> compensation = Map.of(
                "orderId", orderDTO.getOrderId(),
                "clientId", orderDTO.getClientId(),
                "failedStage", failedStage,
                "reason", reason
        );

        rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_COMPENSATION_QUEUE, compensation);
        rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_DLQ, compensation);
        rabbitTemplate.convertAndSend("order.tracking.queue", Map.of(
                "orderId", orderDTO.getOrderId(),
                "clientId", orderDTO.getClientId(),
                "stage", "FAILED_ATTEMPT",
                "details", "ROS failure: " + reason,
                "eventType", "FAILED_ATTEMPT"
        ));
    }
}