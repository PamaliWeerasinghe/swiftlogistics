package com.example.cms_adapter.listener;

import com.example.cms_adapter.dto.OrderDTO;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.http.MediaType;

import java.io.IOException;

@Component
public class CmsAdapterListener {
    private static final String cms_adapter_queue = "cms.adapter.queue";
    private static final String wms_adapter_queue = "wms.adapter.queue";

    private final RabbitTemplate rabbitTemplate;
    private final WebClient webClient;

    private static final String CMS_SOAP_URL = "http://localhost:8080/mock-cms/services/OrderService";

    @Autowired
    public CmsAdapterListener(RabbitTemplate rabbitTemplate){
        this.rabbitTemplate = rabbitTemplate;
        this.webClient = WebClient.builder().build();
    }

    //Get the orders
    @RabbitListener(queues = cms_adapter_queue)
    public void getOrdersFromCmsQueue(OrderDTO orderDTO, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException{
        System.out.println("\n [CMS Adapter] Picked up Order "+orderDTO.getOrderId());

        //translation (JSON -> SOAP XML)
        try{
            System.out.println("\n [CMS Adapter] Translating JSON to SOAP/XML format...");
            String soapXmlPayload = convertToSoapXml(orderDTO);

            System.out.println("\n [CMS Adapter] Sending SOAP/XML to CMS...");

            //Transmit transferred data to the CMS
            String cmsResponse = this.webClient.post()
                    .uri(CMS_SOAP_URL)
                    .contentType(MediaType.TEXT_XML) // Crucial: Informs the legacy server that text data is raw XML
                    .bodyValue(soapXmlPayload)       // Attaches the generated XML payload string
                    .retrieve()                      // Dispatches the network request packet
                    .bodyToMono(String.class)        // Prepares to read the response back as a plain string
                    .block();

            System.out.println("[CMS Adapter] Response to the Legacy CMS" + cmsResponse);
            Thread.sleep(1000);

            //Passing the order to wms_adapter_queue
            rabbitTemplate.convertAndSend(wms_adapter_queue,orderDTO);
            //Use to remove the order from the queue
            channel.basicAck(tag,false);

        }catch (Exception e){
            System.out.println(e.getMessage());
            //Use to add the order back to the wms_adapter_queue
            channel.basicNack(tag, false, true);
        }
    }

    //Convert JSON -> SOAP/XML
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
