package com.example.mock_wms.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@RestController
@CrossOrigin(origins = "*")
public class MockWmsController {

    // Thread-safe in-memory warehouse dispatch ledger
    private final List<Map<String, String>> dispatchLedger = new CopyOnWriteArrayList<>();

    // Feature flag to simulate warehouse floor/API downtime
    private volatile boolean isSystemOnline = true;

    // 1. WAREHOUSE DISPATCH ENDPOINT (Called by WmsAdapterListener on port 8004)
    @PostMapping("/mock-wms/api/dispatch")
    public ResponseEntity<Map<String, Object>> handleWarehouseDispatch(@RequestBody Map<String, Object> orderPayload) {
        if (!isSystemOnline) {
            System.out.println("⚠️ [MOCK WMS] Rejected incoming dispatch — Warehouse System is OFFLINE (503).");
            return ResponseEntity.status(503).body(Map.of(
                    "status", "ERROR",
                    "message", "Warehouse Management System Offline for Maintenance"
            ));
        }

        String orderId = String.valueOf(orderPayload.getOrDefault("orderId", "UNKNOWN"));
        String clientId = String.valueOf(orderPayload.getOrDefault("clientId", "UNKNOWN"));
        String address = String.valueOf(orderPayload.getOrDefault("address", "UNKNOWN"));

        // Simulate warehouse allocation metadata
        String pickingSlipId = "PICK-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        String assignedBay = "BAY-0" + ((dispatchLedger.size() % 6) + 1);
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Map<String, String> record = new ConcurrentHashMap<>();
        record.put("timestamp", timestamp);
        record.put("pickingSlipId", pickingSlipId);
        record.put("orderId", orderId);
        record.put("clientId", clientId);
        record.put("address", address);
        record.put("assignedBay", assignedBay);
        record.put("status", "ALLOCATED & READY");

        dispatchLedger.add(0, record); // Newest dispatches appear at the top

        System.out.println("\n🏭 [MOCK WMS] Order Dispatched -> OrderID: " + orderId +
                " | Slip: " + pickingSlipId + " | Bay: " + assignedBay);

        return ResponseEntity.ok(Map.of(
                "status", "DISPATCH_SCHEDULED",
                "pickingSlipId", pickingSlipId,
                "assignedBay", assignedBay,
                "orderId", orderId,
                "message", "Inventory reserved and loading bay assigned."
        ));
    }

    // 2. FRONTEND DASHBOARD REST ENDPOINTS
    @GetMapping("/api/mock-wms/dispatches")
    public List<Map<String, String>> getDispatches() {
        return dispatchLedger;
    }

    @GetMapping("/api/mock-wms/status")
    public Map<String, Object> getStatus() {
        return Map.of(
                "online", isSystemOnline,
                "totalDispatches", dispatchLedger.size()
        );
    }

    @PostMapping("/api/mock-wms/toggle")
    public Map<String, Boolean> toggleStatus() {
        this.isSystemOnline = !this.isSystemOnline;
        return Map.of("online", isSystemOnline);
    }
}