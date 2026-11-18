package com.example.mock_wms.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@CrossOrigin(origins = "*")
public class MockWmsController {

    private static final Path DISPATCHES_PATH = Path.of("./data/wms-dispatches.json");
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<Map<String, String>> dispatchLedger = new CopyOnWriteArrayList<>();
    private volatile boolean isSystemOnline = true;

    private final ExecutorService socketExecutor = Executors.newCachedThreadPool(r -> {
        Thread thread = new Thread(r, "mock-wms-tcp");
        thread.setDaemon(true);
        return thread;
    });

    @Value("${wms.tcp.port:9090}")
    private int wmsTcpPort;

    @PostConstruct
    public void startTcpServer() {
        loadDispatches();
        socketExecutor.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(wmsTcpPort)) {
                System.out.println("🚢 [MOCK WMS TCP] Listening on port " + wmsTcpPort);
                while (!serverSocket.isClosed()) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        socketExecutor.submit(() -> handleTcpClient(clientSocket));
                    } catch (SocketException e) {
                        if (serverSocket.isClosed()) {
                            break;
                        }
                        throw e;
                    }
                }
            } catch (IOException e) {
                System.err.println("❌ [MOCK WMS TCP] Server failed on port " + wmsTcpPort + ": " + e.getMessage());
            }
        });
    }

    private void loadDispatches() {
        try {
            Files.createDirectories(DISPATCHES_PATH.getParent());
            if (Files.exists(DISPATCHES_PATH)) {
                List<Map<String, String>> loaded = objectMapper.readValue(
                        DISPATCHES_PATH.toFile(), new TypeReference<List<Map<String, String>>>() {});
                dispatchLedger.clear();
                dispatchLedger.addAll(loaded);
            }
        } catch (IOException e) {
            System.err.println("[WMS] Failed to load persisted dispatches: " + e.getMessage());
        }
    }

    private void persistDispatches() {
        try {
            Files.createDirectories(DISPATCHES_PATH.getParent());
            Path temp = DISPATCHES_PATH.resolveSibling(DISPATCHES_PATH.getFileName() + ".tmp");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), dispatchLedger);
            Files.move(temp, DISPATCHES_PATH, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("[WMS] Failed to persist dispatches: " + e.getMessage());
        }
    }

    private void handleTcpClient(Socket clientSocket) {
        try (clientSocket;
             BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8))) {

            String requestLine = reader.readLine();
            if (requestLine == null || requestLine.isBlank()) {
                return;
            }

            String[] parts = requestLine.split("\\|");
            if (parts.length == 0) {
                writer.write("ERR|400|EMPTY_COMMAND\n");
                writer.flush();
                return;
            }

            String command = parts[0];
            if (!isSystemOnline) {
                writer.write("ERR|503|WMS_SYSTEM_OFFLINE\n");
                writer.flush();
                return;
            }

            if ("WMS_DISPATCH".equals(command)) {
                String orderId = parts.length > 1 ? parts[1] : "UNKNOWN";
                String clientId = parts.length > 2 ? parts[2] : "UNKNOWN";
                String itemCount = parts.length > 3 ? parts[3] : "1";
                String address = parts.length > 4 ? String.join("|", Arrays.copyOfRange(parts, 4, parts.length)) : "UNKNOWN";

                String pickingSlipId = "PICK-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
                String assignedBay = "BAY-0" + ((dispatchLedger.size() % 6) + 1);
                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

                Map<String, String> record = new ConcurrentHashMap<>();
                record.put("timestamp", timestamp);
                record.put("pickingSlipId", pickingSlipId);
                record.put("orderId", orderId);
                record.put("clientId", clientId);
                record.put("itemCount", itemCount);
                record.put("address", address);
                record.put("assignedBay", assignedBay);
                record.put("status", "ALLOCATED & READY");

                dispatchLedger.add(0, record);
                persistDispatches();

                System.out.println("\n🏭 [MOCK WMS TCP] Order Dispatched -> OrderID: " + orderId +
                        " | Slip: " + pickingSlipId + " | Bay: " + assignedBay);

                writer.write("ACK|" + orderId + "|WMS_ALLOCATED\n");
                writer.flush();
                return;
            }

            if ("WMS_CANCEL".equals(command)) {
                String orderId = parts.length > 1 ? parts[1] : "UNKNOWN";
                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                boolean updated = false;

                for (Map<String, String> record : dispatchLedger) {
                    if (orderId.equals(record.get("orderId"))) {
                        record.put("status", "CANCELLED");
                        record.put("cancelledAt", timestamp);
                        updated = true;
                    }
                }

                if (!updated) {
                    Map<String, String> record = new ConcurrentHashMap<>();
                    record.put("orderId", orderId);
                    record.put("status", "CANCELLED");
                    record.put("cancelledAt", timestamp);
                    dispatchLedger.add(0, record);
                }
                persistDispatches();

                writer.write("ACK|" + orderId + "|WMS_CANCELLED\n");
                writer.flush();
                return;
            }

            writer.write("ERR|400|UNKNOWN_COMMAND\n");
            writer.flush();
        } catch (IOException e) {
            System.err.println("⚠️ [MOCK WMS TCP] Client communication error: " + e.getMessage());
        }
    }

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

        dispatchLedger.add(0, record);
        persistDispatches();

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