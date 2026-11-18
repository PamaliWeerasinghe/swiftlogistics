package com.example.cms_adapter.listener;

import com.example.cms_adapter.config.RabbitMQConfig;
import com.example.cms_adapter.dto.OrderDTO;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class CmsAdapterListener {
    private static final String cms_adapter_queue = "cms.adapter.queue";
    private static final String wms_adapter_queue = "wms.adapter.queue";
    private static final ConcurrentHashMap<String, Integer> RETRY_COUNTS = new ConcurrentHashMap<>();

    private final RabbitTemplate rabbitTemplate;
    private final WebClient webClient;

    private static final String CMS_SOAP_URL = "http://localhost:8080/mock-cms/services/OrderService";

    @Autowired
    public CmsAdapterListener(RabbitTemplate rabbitTemplate){
        this.rabbitTemplate = rabbitTemplate;
        this.webClient = WebClient.builder().build();
    }

    @RabbitListener(queues = cms_adapter_queue)
    public void getOrdersFromCmsQueue(OrderDTO orderDTO, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.out.println("\n [CMS Adapter] Picked up Order "+orderDTO.getOrderId());

        try {
            System.out.println("\n [CMS Adapter] Translating JSON to SOAP/XML format...");
            String soapXmlPayload = convertToSoapXml(orderDTO);

            System.out.println("\n [CMS Adapter] Sending SOAP/XML to CMS...");

            String cmsResponse = this.webClient.post()
                    .uri(CMS_SOAP_URL)
                    .contentType(MediaType.TEXT_XML)
                    .bodyValue(soapXmlPayload)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            System.out.println("[CMS Adapter] Response to the Legacy CMS" + cmsResponse);

            rabbitTemplate.convertAndSend(wms_adapter_queue, orderDTO);
            RETRY_COUNTS.remove(orderDTO.getOrderId());
            channel.basicAck(tag, false);

        } catch (Exception e) {
            String orderId = orderDTO.getOrderId();
            int attempts = RETRY_COUNTS.merge(orderId, 1, Integer::sum);
            String reason = "CMS SOAP call failed: " + e.getMessage();

            if (attempts <= 3) {
                System.out.println("⚠️ [CMS Adapter] Retry " + attempts + " for order " + orderId + " after temporary failure.");
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                }
                channel.basicNack(tag, false, true);
                return;
            }

            publishCompensation(orderDTO, "CMS", reason);
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
                "details", "CMS failure: " + reason,
                "eventType", "FAILED_ATTEMPT"
        ));
    }

    private String convertToSoapXml(OrderDTO orderDTO){
        return "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ord=\"http://example.com/order\">\n" +
                "   <soapenv:Header/>\n" +
                "   <soapenv:Body>\n" +
                "      <ord:Order>\n" +
                "         <ord:OrderId>" + orderDTO.getOrderId() + "</ord:OrderId>\n" +
                "         <ord:ClientId>" + orderDTO.getClientId() + "</ord:ClientId>\n" +
                "         <ord:Address>" + orderDTO.getAddress() + "</ord:Address>\n" +
                "      </ord:Order>\n" +
                "   </soapenv:Body>\n" +
                "</soapenv:Envelope>";
    }
}
