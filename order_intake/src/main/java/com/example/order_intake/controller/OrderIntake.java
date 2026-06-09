package com.example.order_intake.controller;

import com.example.order_intake.config.RabbitMQConfig;
import com.example.order_intake.dto.OrderDTO;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class OrderIntake {

    private final RabbitTemplate rabbitTemplate;

    @Autowired
    public OrderIntake(RabbitTemplate rabbitTemplate){
        this.rabbitTemplate = rabbitTemplate;
    }

    @PostMapping("/orders")
    public ResponseEntity<Map<String,String>> createOrder(@RequestBody OrderDTO orderDTO){
        try {
            rabbitTemplate.convertAndSend(RabbitMQConfig.order_intake_queue, orderDTO);

            Map<String, String> response = new HashMap<>();
            response.put("status","Order accepted and queued safely for processing");
            response.put("orderId",orderDTO.getOrderId());

            return new ResponseEntity<>(response, HttpStatus.ACCEPTED);

        } catch (Exception e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("status","Failed to accept the order. Please try again..." + e.getMessage());
            return  new ResponseEntity<>(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR);

        }
    }
}
