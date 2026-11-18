package com.example.route_driver.controller;

import com.example.route_driver.config.RabbitMQConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import jakarta.annotation.PostConstruct;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@RestController
@RequestMapping("/api/driver")
@CrossOrigin(origins = "*")
public class RouteDriverController {

    private static final Path DRIVER_MANIFESTS_PATH = Path.of("./data/driver-manifests.json");
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private RabbitTemplate rabbitTemplate;

    private final List<Map<String, Object>> activeManifests = new CopyOnWriteArrayList<>();

    private final List<String> driverRoster = List.of(
            "DRV-101 • Kasun Perera",
            "DRV-102 • Nimal Silva",
            "DRV-103 • Ruwan Fernando"
    );

    @PostConstruct
    public void loadManifests() {
       try {
           Files.createDirectories(DRIVER_MANIFESTS_PATH.getParent());
           if (Files.exists(DRIVER_MANIFESTS_PATH)) {
               List<Map<String, Object>> loaded = objectMapper.readValue(
                       DRIVER_MANIFESTS_PATH.toFile(), new TypeReference<List<Map<String, Object>>>() {});
               activeManifests.clear();
               activeManifests.addAll(loaded);
           }
       } catch (IOException e) {
           System.err.println("[ROUTE DRIVER] Failed to load persisted manifests: " + e.getMessage());
       }
    }

    private void persistManifests() {
       try {
           Files.createDirectories(DRIVER_MANIFESTS_PATH.getParent());
           Path temp = DRIVER_MANIFESTS_PATH.resolveSibling(DRIVER_MANIFESTS_PATH.getFileName() + ".tmp");
           objectMapper.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), activeManifests);
           Files.move(temp, DRIVER_MANIFESTS_PATH, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
       } catch (IOException e) {
           System.err.println("[ROUTE DRIVER] Failed to persist manifests: " + e.getMessage());
       }
    }

    // 1. RABBITMQ LISTENER: Receives optimized route from ROS Adapter
    @RabbitListener(queues = RabbitMQConfig.DRIVER_DISPATCH_QUEUE)
    public void receiveRouteManifest(Map<String, Object> payload, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            String orderId = String.valueOf(payload.getOrDefault("orderId", "UNKNOWN"));
            String clientId = String.valueOf(payload.getOrDefault("clientId", "CLI-DEFAULT"));
            String address = String.valueOf(payload.getOrDefault("address", "Colombo"));
            String assignedDriver = driverRoster.get(activeManifests.size() % driverRoster.size());
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

            double originLat = 6.9388;
            double originLng = 79.8745;
            double destLat = 6.8800 + (Math.abs(address.hashCode()) % 60) / 1000.0;
            double destLng = 79.8550 + (Math.abs(orderId.hashCode()) % 45) / 1000.0;

            Map<String, Object> manifest = new ConcurrentHashMap<>(payload);
            manifest.put("assignedDriver", assignedDriver);
            manifest.put("deliveryStatus", "ASSIGNED");
            manifest.put("podNotes", "Awaiting driver pickup at warehouse bay");
            manifest.put("updatedAt", timestamp);
            manifest.put("originLat", originLat);
            manifest.put("originLng", originLng);
            manifest.put("destLat", destLat);
            manifest.put("destLng", destLng);
            manifest.put("currentLat", originLat);
            manifest.put("currentLng", originLng);

            activeManifests.add(0, manifest);
            persistManifests();

            // Sync initial driver & route coordinates to Notification & Tracking Service (:8007)
            Map<String, Object> trackingEvent = new HashMap<>();
            trackingEvent.put("orderId", orderId);
            trackingEvent.put("clientId", clientId);
            trackingEvent.put("address", address);
            trackingEvent.put("stage", "ROUTE_OPTIMIZED");
            trackingEvent.put("details", "Assigned to " + assignedDriver + " (" + manifest.get("assignedVehicle") + ")");
            trackingEvent.put("assignedDriver", assignedDriver);
            trackingEvent.put("assignedVehicle", String.valueOf(manifest.getOrDefault("assignedVehicle", "VAN-CMB-01")));
            trackingEvent.put("originLat", originLat);
            trackingEvent.put("originLng", originLng);
            trackingEvent.put("destLat", destLat);
            trackingEvent.put("destLng", destLng);
            trackingEvent.put("currentLat", originLat);
            trackingEvent.put("currentLng", originLng);

            rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_TRACKING_QUEUE, trackingEvent);

            System.out.println("\n🚚 [ROUTE & DRIVER SERVICE] Manifest Assigned & Synced to Tracking: " + orderId);
            channel.basicAck(tag, false);
        } catch (Exception e) {
            System.err.println("❌ [ROUTE & DRIVER SERVICE] Error processing manifest: " + e.getMessage());
            channel.basicNack(tag, false, false);
        }
    }

    // 2. MOBILE APP HEALTH ENDPOINT
    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        return Map.of("online", true, "totalManifests", activeManifests.size());
    }

    // 3. MOBILE APP ENDPOINT: Fetch all assigned driver manifests
    @GetMapping("/manifests")
    public List<Map<String, Object>> getManifests() {
        return activeManifests;
    }

    // 3. MOBILE APP ENDPOINT: Driver updates delivery status & Proof of Delivery (PoD)
    @PostMapping("/manifests/{orderId}/status")
    public ResponseEntity<Map<String, Object>> updateDeliveryStatus(
            @PathVariable String orderId,
            @RequestBody Map<String, String> statusUpdate) {

        String newStatus = statusUpdate.getOrDefault("status", "IN_TRANSIT");
        String podNotes = statusUpdate.getOrDefault("podNotes", "Updated via Driver Mobile App");
        String failureReason = statusUpdate.getOrDefault("failureReason", "");
        String signatureBase64 = statusUpdate.getOrDefault("signatureBase64", "");
        String photoProofBase64 = statusUpdate.getOrDefault("photoProofBase64", "");
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

        for (Map<String, Object> item : activeManifests) {
            if (orderId.equals(String.valueOf(item.get("orderId")))) {
                item.put("deliveryStatus", newStatus);
                item.put("podNotes", podNotes);
                item.put("updatedAt", timestamp);
                if (!failureReason.isBlank()) {
                    item.put("failureReason", failureReason);
                }
                if (!signatureBase64.isBlank()) {
                    item.put("signatureBase64", signatureBase64);
                }
                if (!photoProofBase64.isBlank()) {
                    item.put("photoProofBase64", photoProofBase64);
                }

                if ("DELIVERED".equals(newStatus)) {
                    item.put("currentLat", item.get("destLat"));
                    item.put("currentLng", item.get("destLng"));
                }
                if ("FAILED_ATTEMPT".equals(newStatus) && !failureReason.isBlank()) {
                    item.put("currentLat", item.getOrDefault("currentLat", item.get("originLat")));
                    item.put("currentLng", item.getOrDefault("currentLng", item.get("originLng")));
                }
                persistManifests();

                Map<String, Object> trackingEvent = new HashMap<>();
                trackingEvent.put("orderId", orderId);
                trackingEvent.put("clientId", String.valueOf(item.getOrDefault("clientId", "CLI-DEFAULT")));
                trackingEvent.put("address", String.valueOf(item.getOrDefault("address", "")));
                trackingEvent.put("stage", newStatus);
                trackingEvent.put("details", podNotes);
                trackingEvent.put("assignedDriver", item.get("assignedDriver"));
                trackingEvent.put("assignedVehicle", item.get("assignedVehicle"));
                trackingEvent.put("originLat", item.get("originLat"));
                trackingEvent.put("originLng", item.get("originLng"));
                trackingEvent.put("destLat", item.get("destLat"));
                trackingEvent.put("destLng", item.get("destLng"));
                trackingEvent.put("currentLat", item.get("currentLat"));
                trackingEvent.put("currentLng", item.get("currentLng"));
                if (!failureReason.isBlank()) {
                    trackingEvent.put("failureReason", failureReason);
                }
                if (!signatureBase64.isBlank()) {
                    trackingEvent.put("signatureBase64", signatureBase64);
                }
                if (!photoProofBase64.isBlank()) {
                    trackingEvent.put("photoProofBase64", photoProofBase64);
                }

                rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_TRACKING_QUEUE, trackingEvent);
                System.out.println("📱 [DRIVER MOBILE APP -> TRACKING] Order " + orderId + " marked as [" + newStatus + "]");

                return ResponseEntity.ok(item);
            }
        }
        return ResponseEntity.notFound().build();
    }

    @PostMapping("/manifests/{orderId}/priority-reroute")
    public ResponseEntity<Map<String, Object>> priorityReroute(
            @PathVariable String orderId,
            @RequestBody Map<String, Object> reroutePayload) {

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        String priority = String.valueOf(reroutePayload.getOrDefault("priority", "URGENT"));
        String reason = String.valueOf(reroutePayload.getOrDefault("reason", "High-priority reroute requested"));
        String message = String.valueOf(reroutePayload.getOrDefault("message", "Urgent route change initiated"));

        for (Map<String, Object> item : activeManifests) {
            if (orderId.equals(String.valueOf(item.get("orderId")))) {
                item.put("priorityLevel", priority);
                item.put("rerouteReason", reason);
                item.put("deliveryStatus", "ROUTE_UPDATED");
                item.put("updatedAt", timestamp);
                if (reroutePayload.get("address") != null && !String.valueOf(reroutePayload.get("address")).isBlank()) {
                    item.put("address", reroutePayload.get("address"));
                }
                persistManifests();

                Map<String, Object> trackingEvent = new HashMap<>();
                trackingEvent.put("orderId", orderId);
                trackingEvent.put("clientId", String.valueOf(item.getOrDefault("clientId", "CLI-DEFAULT")));
                trackingEvent.put("address", String.valueOf(item.getOrDefault("address", "")));
                trackingEvent.put("stage", "ROUTE_UPDATED");
                trackingEvent.put("details", message);
                trackingEvent.put("assignedDriver", item.get("assignedDriver"));
                trackingEvent.put("assignedVehicle", item.get("assignedVehicle"));
                trackingEvent.put("originLat", item.get("originLat"));
                trackingEvent.put("originLng", item.get("originLng"));
                trackingEvent.put("destLat", item.get("destLat"));
                trackingEvent.put("destLng", item.get("destLng"));
                trackingEvent.put("priorityLevel", priority);
                trackingEvent.put("rerouteReason", reason);
                rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_TRACKING_QUEUE, trackingEvent);

                return ResponseEntity.ok(Map.of(
                        "orderId", orderId,
                        "priorityLevel", priority,
                        "rerouteReason", reason,
                        "message", message,
                        "manifest", item
                ));
            }
        }

        return ResponseEntity.notFound().build();
    }

    // 4. MOBILE APP ENDPOINT: Live GPS Telemetry Stream during IN_TRANSIT
    @PostMapping("/manifests/{orderId}/location")
    public ResponseEntity<Map<String, Object>> updateVehicleLocation(
            @PathVariable String orderId,
            @RequestBody Map<String, Double> coords) {

        Double lat = coords.get("lat");
        Double lng = coords.get("lng");

        for (Map<String, Object> item : activeManifests) {
            if (orderId.equals(String.valueOf(item.get("orderId")))) {
                item.put("currentLat", lat);
                item.put("currentLng", lng);
                persistManifests();

                // Forward silent GPS telemetry update to Notification & Tracking Service (:8007)
                Map<String, Object> gpsEvent = new HashMap<>();
                gpsEvent.put("orderId", orderId);
                gpsEvent.put("clientId", String.valueOf(item.getOrDefault("clientId", "CLI-DEFAULT")));
                gpsEvent.put("eventType", "GPS_TELEMETRY");
                gpsEvent.put("currentLat", lat);
                gpsEvent.put("currentLng", lng);

                rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_TRACKING_QUEUE, gpsEvent);
                return ResponseEntity.ok(item);
            }
        }
        return ResponseEntity.notFound().build();
    }
}