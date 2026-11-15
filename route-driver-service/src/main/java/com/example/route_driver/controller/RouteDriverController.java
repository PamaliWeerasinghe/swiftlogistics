package com.example.route_driver.controller;

import com.example.route_driver.config.RabbitMQConfig;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@RestController
@RequestMapping("/api/driver")
@CrossOrigin(origins = "*")
public class RouteDriverController {

    private final List> activeManifests = new CopyOnWriteArrayList<>();

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
            String assignedDriver = driverRoster.get(activeManifests.size() % driverRoster.size());
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

            Map<String, Object> manifest = new ConcurrentHashMap<>(payload);
            manifest.put("assignedDriver", assignedDriver);
            manifest.put("deliveryStatus", "ASSIGNED");
            manifest.put("podNotes", "Awaiting driver pickup at warehouse bay");
            manifest.put("updatedAt", timestamp);

            activeManifests.add(0, manifest);

            System.out.println("\n🚚 [ROUTE & DRIVER SERVICE] New Manifest Assigned!");
            System.out.println("   Order: " + orderId + " | Route: " + manifest.get("routeId") + " | Driver: " + assignedDriver);

            channel.basicAck(tag, false);
        } catch (Exception e) {
            System.err.println("❌ [ROUTE & DRIVER SERVICE] Error processing manifest: " + e.getMessage());
            channel.basicNack(tag, false, false);
        }
    }

    // 2. MOBILE APP ENDPOINT: Fetch all assigned driver manifests
    @GetMapping("/manifests")
    public List> getManifests() {
        return activeManifests;
    }

    // 3. MOBILE APP ENDPOINT: Driver updates delivery status & Proof of Delivery (PoD)
    @PostMapping("/manifests/{orderId}/status")
    public ResponseEntity> updateDeliveryStatus(
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

                System.out.println("📱 [DRIVER MOBILE APP] Order " + orderId + " marked as [" + newStatus + "] — Notes: " + podNotes);
                return ResponseEntity.ok(item);
            }
        }
        return ResponseEntity.notFound().build();
    }
}