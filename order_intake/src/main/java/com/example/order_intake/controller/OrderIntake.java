package com.example.order_intake.controller;

import com.example.order_intake.config.RabbitMQConfig;
import com.example.order_intake.dto.OrderDTO;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@RestController
@RequestMapping("/api/v1")
public class OrderIntake {

    private static final Path ORDERS_PATH = Path.of("./data/orders.json");
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<Map<String, Object>> orders = new CopyOnWriteArrayList<>();
    private final RabbitTemplate rabbitTemplate;

    @Autowired
    public OrderIntake(RabbitTemplate rabbitTemplate){
        this.rabbitTemplate = rabbitTemplate;
    }

    @PostConstruct
    public void loadOrders() {
        try {
            Files.createDirectories(ORDERS_PATH.getParent());
            if (Files.exists(ORDERS_PATH)) {
                List<Map<String, Object>> loaded = objectMapper.readValue(
                        ORDERS_PATH.toFile(),
                        new TypeReference<List<Map<String, Object>>>() {}
                );
                orders.clear();
                orders.addAll(loaded);
            }
        } catch (IOException e) {
            System.err.println("[ORDER INTAKE] Failed to load persisted orders: " + e.getMessage());
        }
    }

    @PostMapping("/orders")
    public ResponseEntity<Map<String,String>> createOrder(@RequestBody OrderDTO orderDTO){
        if (orderDTO == null || orderDTO.getOrderId() == null || orderDTO.getOrderId().isBlank()
                || orderDTO.getClientId() == null || orderDTO.getClientId().isBlank()
                || orderDTO.getAddress() == null || orderDTO.getAddress().isBlank()) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("status", "Validation failed: orderId, clientId, and address are required.");
            return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
        }

        try {
            rabbitTemplate.convertAndSend(RabbitMQConfig.order_intake_queue, orderDTO);

            Map<String, Object> record = new HashMap<>();
            record.put("orderId", orderDTO.getOrderId());
            record.put("clientId", orderDTO.getClientId());
            record.put("address", orderDTO.getAddress());
            record.put("status", "ACCEPTED");
            orders.add(0, record);
            persistOrders();

            Map<String, String> response = new HashMap<>();
            response.put("status","Order accepted and queued safely for processing");
            response.put("orderId", orderDTO.getOrderId());

            return new ResponseEntity<>(response, HttpStatus.ACCEPTED);

        } catch (Exception e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("status","Failed to accept the order. Please try again..." + e.getMessage());
            return new ResponseEntity<>(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/orders")
    public List<Map<String, Object>> getOrders() {
        return new ArrayList<>(orders);
    }

    @GetMapping("/orders/{orderId}")
    public ResponseEntity<Map<String, Object>> getOrder(@PathVariable String orderId) {
        for (Map<String, Object> order : orders) {
            if (orderId.equals(order.get("orderId"))) {
                return ResponseEntity.ok(order);
            }
        }
        return ResponseEntity.notFound().build();
    }

    private void persistOrders() {
        try {
            Files.createDirectories(ORDERS_PATH.getParent());
            Path temp = ORDERS_PATH.resolveSibling(ORDERS_PATH.getFileName() + ".tmp");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), orders);
            Files.move(temp, ORDERS_PATH, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("[ORDER INTAKE] Failed to persist orders: " + e.getMessage());
        }
    }
}
