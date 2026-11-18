package com.example.wms_adapter.listener;

import com.example.wms_adapter.config.RabbitMQConfig;
import com.example.wms_adapter.dto.OrderDTO;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class WmsAdapterListener {

    private static final String WMS_ADAPTER_QUEUE = "wms.adapter.queue";
    private static final String WMS_TCP_HOST = "localhost";
    private static final int WMS_TCP_PORT = 9090;
    private static final int SOCKET_TIMEOUT_MS = 3000;
    private static final ConcurrentHashMap<String, Integer> RETRY_COUNTS = new ConcurrentHashMap<>();

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @RabbitListener(queues = WMS_ADAPTER_QUEUE)
    public void processWarehouseOrder(OrderDTO orderDTO, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.out.println("\n📦 [WMS ADAPTER] Picked up Order: " + orderDTO.getOrderId() + " from Queue.");

        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(WMS_TCP_HOST, WMS_TCP_PORT), SOCKET_TIMEOUT_MS);
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);

            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {

            String dispatchCommand = "WMS_DISPATCH|" + orderDTO.getOrderId() + "|" + orderDTO.getClientId() + "|1|" + orderDTO.getAddress() + "\n";
            System.out.println("🔌 [WMS ADAPTER] Sending dispatch to WMS TCP socket at localhost:9090");

            writer.write(dispatchCommand);
            writer.flush();

            String response = reader.readLine();
            System.out.println("✔ [WMS ADAPTER] Mock warehouse response: " + response);

            if (response != null && response.startsWith("ACK|")) {
                rabbitTemplate.convertAndSend("order.tracking.queue", Map.of(
                        "orderId", orderDTO.getOrderId(),
                        "clientId", orderDTO.getClientId(),
                        "address", orderDTO.getAddress(),
                        "stage", "WMS_ALLOCATED",
                        "details", "Warehouse allocated order for pick and dispatch"
                ));

                rabbitTemplate.convertAndSend(RabbitMQConfig.ROS_ADAPTER_QUEUE, orderDTO);
                RETRY_COUNTS.remove(orderDTO.getOrderId());
                System.out.println("➡️ [WMS ADAPTER] Forwarded Order " + orderDTO.getOrderId() + " to ros.adapter.queue");

                channel.basicAck(tag, false);
                System.out.println("🎉 [WMS ADAPTER] RabbitMQ ACK sent. Handed off to ROS stage!");
                return;
            }

            if (response != null && response.startsWith("ERR|")) {
                throw new IOException("WMS rejected dispatch: " + response);
            }

            throw new IOException("WMS protocol error: empty or unexpected response: " + response);

            }
        } catch (IOException e) {
            String orderId = orderDTO.getOrderId();
            int attempts = RETRY_COUNTS.merge(orderId, 1, Integer::sum);
            String reason = "WMS socket failure: " + e.getMessage();

            if (attempts <= 3) {
                System.out.println("⚠️ [WMS ADAPTER] Retry " + attempts + " for order " + orderId + " after temporary failure.");
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                }
                channel.basicNack(tag, false, true);
                return;
            }

            publishCompensation(orderDTO, "WMS", reason);
            RETRY_COUNTS.remove(orderId);
            channel.basicAck(tag, false);
        }
    }

    private void publishCompensation(OrderDTO orderDTO, String failedStage, String reason) {
        Map<String, Object> compensation = Map.of(
                "orderId", orderDTO.getOrderId(),
                "clientId", orderDTO.getClientId(),
                "failedStage", failedStage,
                "reason", reason
        );

        rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_COMPENSATION_QUEUE, compensation);
        rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_DLQ, compensation);
        rabbitTemplate.convertAndSend("order.tracking.queue", Map.of(
                "orderId", orderDTO.getOrderId(),
                "clientId", orderDTO.getClientId(),
                "stage", "FAILED_ATTEMPT",
                "details", "WMS failure: " + reason,
                "eventType", "FAILED_ATTEMPT"
        ));
    }
}