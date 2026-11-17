package com.example.route_driver.controller;

import com.example.route_driver.config.RabbitMQConfig;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
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

    @Autowired
    private RabbitTemplate rabbitTemplate; // <-- 1. INJECT RABBIT TEMPLATE

    private final List<Map<String, Object>> activeManifests = new CopyOnWriteArrayList<>();

    private final List<String> driverRoster = List.of(
            "DRV-101 • Kasun Perera",
            "DRV-102 • Nimal Silva",
            "DRV-103 • Ruwan Fernando"
    );

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

    // 2. MOBILE APP ENDPOINT: Fetch all assigned driver manifests
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
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

        for (Map<String, Object> item : activeManifests) {
            if (orderId.equals(String.valueOf(item.get("orderId")))) {
                item.put("deliveryStatus", newStatus);
                item.put("podNotes", podNotes);
                item.put("updatedAt", timestamp);

                // If DELIVERED, snap current coordinates to destination
                if ("DELIVERED".equals(newStatus)) {
                    item.put("currentLat", item.get("destLat"));
                    item.put("currentLng", item.get("destLng"));
                }

                // ====================================================================
                // PUBLISH DRIVER STATUS & PoD TO order.tracking.queue (:8007)
                // ====================================================================
                Map<String, Object> trackingEvent = new HashMap<>();
                trackingEvent.put("orderId", orderId);
                trackingEvent.put("clientId", String.valueOf(item.getOrDefault("clientId", "CLI-DEFAULT")));
                trackingEvent.put("address", String.valueOf(item.getOrDefault("address", "")));
                trackingEvent.put("stage", newStatus); // IN_TRANSIT, DELIVERED, or FAILED_ATTEMPT
                trackingEvent.put("details", podNotes);
                trackingEvent.put("assignedDriver", item.get("assignedDriver"));
                trackingEvent.put("assignedVehicle", item.get("assignedVehicle"));
                trackingEvent.put("originLat", item.get("originLat"));
                trackingEvent.put("originLng", item.get("originLng"));
                trackingEvent.put("destLat", item.get("destLat"));
                trackingEvent.put("destLng", item.get("destLng"));
                trackingEvent.put("currentLat", item.get("currentLat"));
                trackingEvent.put("currentLng", item.get("currentLng"));

                rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_TRACKING_QUEUE, trackingEvent);
                System.out.println("📱 [DRIVER MOBILE APP -> TRACKING] Order " + orderId + " marked as [" + newStatus + "]");

                return ResponseEntity.ok(item);
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