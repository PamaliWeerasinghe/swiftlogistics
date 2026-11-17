package com.example.notification_tracking.controller;

import com.example.notification_tracking.config.RabbitMQConfig;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@RestController
@RequestMapping("/api/tracking")
@CrossOrigin(origins = "*")
public class TrackingController {

    private final Map<String, Map<String, Object>> orderTrackingStore = new ConcurrentHashMap<>();
    private final List<Map<String, Object>> notificationFeed = new CopyOnWriteArrayList<>();

    private static final Map<String, Integer> STAGE_RANK = Map.of(
            "ORDER_RECEIVED", 1,
            "CMS_VERIFIED", 2,
            "WMS_ALLOCATED", 3,
            "ROUTE_OPTIMIZED", 4,
            "IN_TRANSIT", 5,
            "DELIVERED", 6,
            "FAILED_ATTEMPT", 6
    );

    @RabbitListener(queues = RabbitMQConfig.ORDER_TRACKING_QUEUE)
    @SuppressWarnings("unchecked")
    public void consumeTrackingEvent(Map<String, Object> event, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            String orderId = String.valueOf(event.getOrDefault("orderId", "UNKNOWN"));
            String eventType = String.valueOf(event.getOrDefault("eventType", "STAGE_UPDATE"));

            // 1. Handle high-frequency GPS Telemetry updates silently (no SMS spam)
            if ("GPS_TELEMETRY".equals(eventType)) {
                Map<String, Object> existing = orderTrackingStore.get(orderId);
                if (existing != null) {
                    if (event.get("currentLat") != null) existing.put("currentLat", event.get("currentLat"));
                    if (event.get("currentLng") != null) existing.put("currentLng", event.get("currentLng"));
                }
                channel.basicAck(tag, false);
                return;
            }

            // 2. Handle standard Lifecycle Stage & Driver App PoD updates
            String clientId = String.valueOf(event.getOrDefault("clientId", "CLI-DEFAULT"));
            String stage = String.valueOf(event.getOrDefault("stage", "ORDER_RECEIVED"));
            String details = String.valueOf(event.getOrDefault("details", "Status updated"));
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            orderTrackingStore.compute(orderId, (key, existing) -> {
                if (existing == null) {
                    existing = new ConcurrentHashMap<>();
                    existing.put("orderId", orderId);
                    existing.put("clientId", clientId);
                    existing.put("currentStage", stage);
                    existing.put("currentRank", STAGE_RANK.getOrDefault(stage, 1));
                    existing.put("latestDetails", details);
                    existing.put("updatedAt", timestamp);
                    existing.put("history", new CopyOnWriteArrayList<Map<String, Object>>());
                } else {
                    int incomingRank = STAGE_RANK.getOrDefault(stage, 1);
                    int currentRank = (int) existing.getOrDefault("currentRank", 0);
                    if (incomingRank >= currentRank) {
                        existing.put("currentStage", stage);
                        existing.put("currentRank", incomingRank);
                        existing.put("latestDetails", details);
                        existing.put("updatedAt", timestamp);
                    }
                }

                // Copy optional Driver & GPS metadata if present in event
                for (String metaKey : List.of("address", "assignedDriver", "assignedVehicle",
                        "originLat", "originLng", "destLat", "destLng", "currentLat", "currentLng")) {
                    if (event.get(metaKey) != null) {
                        existing.put(metaKey, event.get(metaKey));
                    }
                }

                List<Map<String, Object>> history = (List<Map<String, Object>>) existing.get("history");
                history.add(0, Map.of(
                        "stage", stage,
                        "details", details,
                        "timestamp", timestamp
                ));
                return existing;
            });

            String alertMsg = String.format("[SMS/Email to %s] Order %s -> %s: %s", clientId, orderId, stage, details);
            notificationFeed.add(0, Map.of(
                    "timestamp", timestamp,
                    "orderId", orderId,
                    "clientId", clientId,
                    "stage", stage,
                    "message", alertMsg
            ));

            System.out.println("🔔 [NOTIFICATION & TRACKING] " + alertMsg);
            channel.basicAck(tag, false);

        } catch (Exception e) {
            System.err.println("❌ [NOTIFICATION & TRACKING] Failed to process event: " + e.getMessage());
            channel.basicNack(tag, false, false);
        }
    }

    @GetMapping("/orders")
    public Collection<Map<String, Object>> getAllTrackedOrders() {
        return orderTrackingStore.values();
    }

    @GetMapping("/orders/{orderId}")
    public ResponseEntity<Map<String, Object>> getOrderTimeline(@PathVariable String orderId) {
        Map<String, Object> record = orderTrackingStore.get(orderId);
        return record != null ? ResponseEntity.ok(record) : ResponseEntity.notFound().build();
    }

    @GetMapping("/notifications")
    public List<Map<String, Object>> getNotifications() {
        return notificationFeed;
    }
}