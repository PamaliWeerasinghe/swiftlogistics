package com.example.mock_ros.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@RestController
@CrossOrigin(origins = "*")
public class RosCloudController {

    @Value("${ros.vendor.api-key}")
    private String expectedApiKey;

    // Thread-safe list storing generated routes for the live dashboard
    private final List> optimizedRoutes = new CopyOnWriteArrayList<>();

    // Simulated third-party fleet availability pool
    private final List<String> availableFleet = List.of(
            "VAN-CMB-01 (Colombo North)",
            "VAN-CMB-04 (Colombo Central)",
            "TRUCK-WP-02 (Western Express)"
    );

    // Feature flag to simulate 503 cloud downtime
    private volatile boolean isCloudOnline = true;

    // 1. CLOUD REST API ENDPOINT (Called by ros-adapter on port 8005)
    @PostMapping("/api/v1/routes/optimize")
    public ResponseEntity> optimizeRoute(
            @RequestHeader(value = "X-API-Key", required = false) String apiKey,
            @RequestBody Map<String, Object> requestPayload) {

        if (!isCloudOnline) {
            System.out.println("⚠️ [MOCK ROS CLOUD] Rejected request — Cloud API toggled OFFLINE (503).");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                    "error", "CLOUD_SERVICE_UNAVAILABLE",
                    "message", "Third-party ROS API is currently undergoing maintenance."
            ));
        }

        if (apiKey == null || !apiKey.equals(expectedApiKey)) {
            System.out.println("❌ [MOCK ROS CLOUD] Unauthorized request — Missing or invalid X-API-Key.");
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of(
                    "error", "INVALID_API_KEY",
                    "message", "Missing or invalid X-API-Key header."
            ));
        }

        String orderId = String.valueOf(requestPayload.getOrDefault("orderId", "UNKNOWN"));
        String clientId = String.valueOf(requestPayload.getOrDefault("clientId", "UNKNOWN"));
        String destination = String.valueOf(requestPayload.getOrDefault("deliveryAddress", "UNKNOWN"));

        // Calculate simulated route metrics and assign a vehicle
        String routeId = "RT-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        String assignedVehicle = availableFleet.get(optimizedRoutes.size() % availableFleet.size());
        double estimatedDistanceKm = Math.round((4.5 + (Math.abs(destination.hashCode()) % 180) / 10.0) * 10.0) / 10.0;
        int estimatedEtaMins = (int) Math.round(estimatedDistanceKm * 3.2);
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Map<String, Object> routeRecord = new ConcurrentHashMap<>();
        routeRecord.put("timestamp", timestamp);
        routeRecord.put("routeId", routeId);
        routeRecord.put("orderId", orderId);
        routeRecord.put("clientId", clientId);
        routeRecord.put("destination", destination);
        routeRecord.put("assignedVehicle", assignedVehicle);
        routeRecord.put("distanceKm", estimatedDistanceKm + " km");
        routeRecord.put("etaMins", estimatedEtaMins + " mins");
        routeRecord.put("waypoints", List.of("SwiftLogistics Central Hub", "Colombo Ring Junction", destination));

        optimizedRoutes.add(0, routeRecord);

        System.out.println("🛰️ [MOCK ROS CLOUD] Route Optimized -> RouteID: " + routeId +
                " | OrderID: " + orderId + " | Vehicle: " + assignedVehicle);

        return ResponseEntity.ok(Map.of(
                "status", "OPTIMIZED",
                "routeId", routeId,
                "orderId", orderId,
                "assignedVehicle", assignedVehicle,
                "estimatedDistanceKm", estimatedDistanceKm,
                "estimatedEtaMinutes", estimatedEtaMins
        ));
    }

    // 2. DASHBOARD UI ENDPOINTS (Polled by index.html)
    @GetMapping("/api/mock-ros/routes")
    public List> getRoutes() {
        return optimizedRoutes;
    }

    @GetMapping("/api/mock-ros/status")
    public Map<String, Object> getStatus() {
        return Map.of("online", isCloudOnline, "totalRoutes", optimizedRoutes.size());
    }

    @PostMapping("/api/mock-ros/toggle")
    public Map<String, Boolean> toggleStatus() {
        this.isCloudOnline = !this.isCloudOnline;
        return Map.of("online", isCloudOnline);
    }
}