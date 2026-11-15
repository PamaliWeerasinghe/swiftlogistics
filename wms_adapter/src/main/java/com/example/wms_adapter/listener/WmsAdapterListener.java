package com.example.wms_adapter.listener;

import com.example.wms_adapter.config.RabbitMQConfig;
import com.example.wms_adapter.dto.OrderDTO;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate; // <-- ADD IMPORT
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired; // <-- ADD IMPORT
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

@Component
public class WmsAdapterListener {

    private static final String WMS_ADAPTER_QUEUE = "wms.adapter.queue";
    private static final String WMS_TCP_HOST = "localhost";
    private static final int WMS_TCP_PORT = 9090;

    // 1. INJECT RABBIT TEMPLATE (ADD THIS)
    @Autowired
    private RabbitTemplate rabbitTemplate;

    @RabbitListener(queues = WMS_ADAPTER_QUEUE)
    public void processWarehouseOrder(OrderDTO orderDTO, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.out.println("\n📦 [WMS ADAPTER] Picked up Order: " + orderDTO.getOrderId() + " from Queue.");

        String tcpPayload = String.format("DISPATCH|%s|%s|%s",
                orderDTO.getOrderId(),
                orderDTO.getClientId(),
                orderDTO.getAddress().replace("|", " "));

        System.out.println("🔌 [WMS ADAPTER] Opening raw TCP/IP socket to " + WMS_TCP_HOST + ":" + WMS_TCP_PORT);

        try (Socket socket = new Socket(WMS_TCP_HOST, WMS_TCP_PORT);
             PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {

            socket.setSoTimeout(5000);
            out.println(tcpPayload);
            System.out.println("➡️ [WMS ADAPTER] Sent TCP Frame: " + tcpPayload);

            String tcpResponse = in.readLine();

            if (tcpResponse != null && tcpResponse.startsWith("ACK|")) {
                System.out.println("✔ [WMS ADAPTER] Warehouse TCP Response: " + tcpResponse);

                // 2. FORWARD TO ROS QUEUE BEFORE ACKNOWLEDGING (ADD THESE 2 LINES)
                rabbitTemplate.convertAndSend(RabbitMQConfig.ROS_ADAPTER_QUEUE, orderDTO);
                System.out.println("➡️ [WMS ADAPTER] Forwarded Order " + orderDTO.getOrderId() + " to ros.adapter.queue");

                // 3. ACKNOWLEDGE WMS QUEUE
                channel.basicAck(tag, false);
                System.out.println("🎉 [WMS ADAPTER] RabbitMQ ACK sent. Handed off to ROS stage!");
            } else {
                throw new IOException("Warehouse rejected or returned invalid TCP response: " + tcpResponse);
            }

        } catch (Exception e) {
            System.out.println("❌ [WMS ADAPTER] TCP/IP Transmission Failed: " + e.getMessage());
            channel.basicNack(tag, false, true);
        }
    }
}