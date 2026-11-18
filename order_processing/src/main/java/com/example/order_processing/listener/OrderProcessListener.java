package com.example.order_processing.listener;

import com.example.order_processing.config.RabbitMQConfig;
import com.example.order_processing.dto.OrderDTO;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Component
public class OrderProcessListener {

    private static final String order_intake_queue = "order.intake.queue";
    private static final String cms_adapter_queue = "cms.adapter.queue";
    private static final String trackingQueue = "order.tracking.queue";
    private final WebClient webClient = WebClient.builder().build();

    private final RabbitTemplate rabbitTemplate;

    @Autowired
    public OrderProcessListener(RabbitTemplate rabbitTemplate){
        this.rabbitTemplate = rabbitTemplate;
    }

    @RabbitListener(queues = order_intake_queue)
    public void orderListener(OrderDTO orderDTO, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.out.println("\n [ORDER LISTENER] Grabbed Order: "+ orderDTO.getOrderId());
        try {
            rabbitTemplate.convertAndSend(cms_adapter_queue, orderDTO);
            channel.basicAck(tag, false);
            System.out.println("Handed over to cms.adapter.queue");
        } catch (Exception e) {
            System.out.println("❌ [ORDER LISTENER] Failed to send order to CMS: " + e.getMessage());
            channel.basicNack(tag, false, false);
        }
    }

    @RabbitListener(queues = RabbitMQConfig.ORDER_COMPENSATION_QUEUE)
    public void handleCompensation(Map<String, Object> compensation, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            String orderId = String.valueOf(compensation.getOrDefault("orderId", "UNKNOWN"));
            String clientId = String.valueOf(compensation.getOrDefault("clientId", "UNKNOWN"));
            String failedStage = String.valueOf(compensation.getOrDefault("failedStage", "UNKNOWN"));
            String reason = String.valueOf(compensation.getOrDefault("reason", "Unknown failure"));

            if ("WMS".equals(failedStage)) {
                webClient.post()
                        .uri("http://localhost:8080/api/mock-cms/compensate/" + orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(Map.of("orderId", orderId, "clientId", clientId, "reason", reason))
                        .retrieve()
                        .toBodilessEntity()
                        .block();
            }

            if ("ROS".equals(failedStage)) {
                webClient.post()
                        .uri("http://localhost:8080/api/mock-cms/compensate/" + orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(Map.of("orderId", orderId, "clientId", clientId, "reason", reason))
                        .retrieve()
                        .toBodilessEntity()
                        .block();

                try (Socket socket = new Socket("localhost", 9090);
                     BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
                    socket.setSoTimeout(3000);
                    writer.write("WMS_CANCEL|" + orderId + "\n");
                    writer.flush();
                }
            }

            String message = "Saga compensation executed for order " + orderId + " after failure in " + failedStage + ". CMS contract and WMS allocation were rolled back.";
            rabbitTemplate.convertAndSend(trackingQueue, Map.of(
                    "orderId", orderId,
                    "clientId", clientId,
                    "stage", "ROLLED_BACK",
                    "details", message,
                    "eventType", "ROLLED_BACK"
            ));

            channel.basicAck(tag, false);
        } catch (Exception e) {
            System.err.println("❌ [ORDER LISTENER] Compensation failed: " + e.getMessage());
            channel.basicNack(tag, false, false);
        }
    }
}
