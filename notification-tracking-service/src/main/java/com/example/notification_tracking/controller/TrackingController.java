package com.example.notification_tracking.controller;

import com.example.notification_tracking.config.RabbitMQConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import jakarta.annotation.PostConstruct;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@RestController
@RequestMapping("/api/tracking")
@CrossOrigin(origins = "*")
public class TrackingController {

    private static final Path TRACKING_STATE_PATH = Path.of("./data/tracking-state.json");
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RabbitTemplate rabbitTemplate;
    private final Map<String, Map<String, Object>> orderTrackingStore = new ConcurrentHashMap<>();
    private final List<Map<String, Object>> notificationFeed = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<SseEmitter> activeSubscribers = new CopyOnWriteArrayList<>();

    @Autowired
    public TrackingController(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    @PostConstruct
    public void loadState() {
        try {
            Files.createDirectories(TRACKING_STATE_PATH.getParent());
            if (Files.exists(TRACKING_STATE_PATH)) {
                Map<String, Object> loaded = objectMapper.readValue(
                        TRACKING_STATE_PATH.toFile(), new TypeReference<Map<String, Object>>() {});
                Object orders = loaded.get("orders");
                Object notifications = loaded.get("notifications");
                if (orders instanceof Map<?, ?> orderMap) {
                    orderTrackingStore.clear();
                    for (Map.Entry<?, ?> entry : orderMap.entrySet()) {
                        Object value = entry.getValue();
                        if (value instanceof Map<?, ?>) {
                            orderTrackingStore.put(String.valueOf(entry.getKey()), (Map<String, Object>) value);
                        }
                    }
                }
                if (notifications instanceof List<?> list) {
                    notificationFeed.clear();
                    for (Object item : list) {
                        if (item instanceof Map<?, ?> map) {
                            notificationFeed.add((Map<String, Object>) map);
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("[TRACKING] Failed to load persisted state: " + e.getMessage());
        }
    }

    private void persistState() {
        try {
            Files.createDirectories(TRACKING_STATE_PATH.getParent());
            Path temp = TRACKING_STATE_PATH.resolveSibling(TRACKING_STATE_PATH.getFileName() + ".tmp");
            Map<String, Object> state = new HashMap<>();
            state.put("orders", orderTrackingStore);
            state.put("notifications", notificationFeed);
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), state);
            Files.move(temp, TRACKING_STATE_PATH, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("[TRACKING] Failed to persist state: " + e.getMessage());
        }
    }

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
                    broadcastTrackingUpdate(existing, null);
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
            Map<String, Object> latestNotification = new HashMap<>();
            latestNotification.put("timestamp", timestamp);
            latestNotification.put("orderId", orderId);
            latestNotification.put("clientId", clientId);
            latestNotification.put("stage", stage);
            latestNotification.put("message", alertMsg);
            notificationFeed.add(0, latestNotification);
            Map<String, Object> updatedOrder = orderTrackingStore.get(orderId);
            persistState();
            broadcastTrackingUpdate(updatedOrder, latestNotification);

            System.out.println("🔔 [NOTIFICATION & TRACKING] " + alertMsg);
            channel.basicAck(tag, false);

        } catch (Exception e) {
            Map<String, Object> failedEvent = new HashMap<>();
            failedEvent.put("orderId", event.getOrDefault("orderId", "UNKNOWN"));
            failedEvent.put("clientId", event.getOrDefault("clientId", "CLI-DEFAULT"));
            failedEvent.put("failedStage", "TRACKING");
            failedEvent.put("reason", e.getMessage());
            failedEvent.put("event", event);

            System.err.println("❌ [NOTIFICATION & TRACKING] Failed to process event: " + e.getMessage());
            rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_DLQ, failedEvent);
            persistState();
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

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamTrackingUpdates() {
        SseEmitter emitter = new SseEmitter(0L);
        activeSubscribers.add(emitter);
        emitter.onCompletion(() -> activeSubscribers.remove(emitter));
        emitter.onTimeout(() -> {
            activeSubscribers.remove(emitter);
            emitter.complete();
        });
        emitter.onError((ex) -> activeSubscribers.remove(emitter));

        try {
            Map<String, Object> initialPayload = new HashMap<>();
            initialPayload.put("orders", orderTrackingStore);
            initialPayload.put("notifications", notificationFeed);
            emitter.send(SseEmitter.event().name("tracking-update").data(initialPayload));
        } catch (IOException e) {
            activeSubscribers.remove(emitter);
            emitter.completeWithError(e);
        }

        return emitter;
    }

    private void broadcastTrackingUpdate(Map<String, Object> updatedOrder, Map<String, Object> latestNotification) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("type", "tracking-update");
        if (updatedOrder != null) {
            payload.put("order", updatedOrder);
        }
        if (latestNotification != null) {
            payload.put("notification", latestNotification);
        }
        payload.put("orders", orderTrackingStore);
        payload.put("notifications", notificationFeed);

        for (SseEmitter emitter : activeSubscribers) {
            try {
                emitter.send(SseEmitter.event().name("tracking-update").data(payload));
            } catch (IOException e) {
                activeSubscribers.remove(emitter);
            }
        }
    }
}