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

    // Stores current state + timeline history per orderId
    private final Map<String, Map<String, Object>> orderTrackingStore = new ConcurrentHashMap<>();

    // Stores dispatched SMS/Email notifications for the live log
    private final List> notificationFeed = new CopyOnWriteArrayList<>();

    // Monotonic rank prevents out-of-order Competing Consumer threads from regressing status
    private static final Map<String, Integer> STAGE_RANK = Map.of(
            "ORDER_RECEIVED", 1,
            "CMS_VERIFIED", 2,
            "WMS_ALLOCATED", 3,
            "ROUTE_OPTIMIZED", 4,
            "IN_TRANSIT", 5,
            "DELIVERED", 6,
            "FAILED_ATTEMPT", 6
    );

    // 1. RABBITMQ LISTENER: Consumes events from all pipeline microservices
    @RabbitListener(queues = RabbitMQConfig.ORDER_TRACKING_QUEUE)
    @SuppressWarnings("unchecked")
    public void consumeTrackingEvent(Map<String, Object> event, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            String orderId = String.valueOf(event.getOrDefault("orderId", "UNKNOWN"));
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
                    existing.put("history", new CopyOnWriteArrayList>());
                } else {
                    int incomingRank = STAGE_RANK.getOrDefault(stage, 1);
                    int currentRank = (int) existing.getOrDefault("currentRank", 0);
                    // Only advance top-level status if incoming stage is newer or equal
                    if (incomingRank >= currentRank) {
                        existing.put("currentStage", stage);
                        existing.put("currentRank", incomingRank);
                        existing.put("latestDetails", details);
                        existing.put("updatedAt", timestamp);
                    }
                }

                List> history = (List>) existing.get("history");
                history.add(0, Map.of(
                        "stage", stage,
                        "details", details,
                        "timestamp", timestamp
                ));
                return existing;
            });

            // Trigger Customer Notification (SMS / Email simulation)
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

    // 2. REST API: Get all tracked orders
    @GetMapping("/orders")
    public Collection> getAllTrackedOrders() {
        return orderTrackingStore.values();
    }

    // 3. REST API: Get specific order tracking timeline by orderId
    @GetMapping("/orders/{orderId}")
    public ResponseEntity> getOrderTimeline(@PathVariable String orderId) {
        Map<String, Object> record = orderTrackingStore.get(orderId);
        return record != null ? ResponseEntity.ok(record) : ResponseEntity.notFound().build();
    }

    // 4. REST API: Get live SMS/Email notification feed
    @GetMapping("/notifications")
    public List> getNotifications() {
        return notificationFeed;
    }
}