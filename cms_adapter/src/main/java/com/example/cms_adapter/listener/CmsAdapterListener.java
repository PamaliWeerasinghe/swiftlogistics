package com.example.cms_adapter.listener;

import com.example.cms_adapter.dto.OrderDTO;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class CmsAdapterListener {
    private static final String cms_adapter_queue = "cms.adapter.queue";
    private static final String wms_adapter_queue = "wms.adapter.queue";

    private final RabbitTemplate rabbitTemplate;

    @Autowired
    public CmsAdapterListener(RabbitTemplate rabbitTemplate){
        this.rabbitTemplate = rabbitTemplate;
    }

    //Get the orders
    public void getOrdersFromCmsQueue(OrderDTO orderDTO, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException{
        System.out.println("\n [CMS Adapter] Picked up Order "+orderDTO.getOrderId());
        //translation
        try{
            System.out.println("\n [CMS Adapter] Translating JSON to SOAP/XML format...");
            String soapXmlPayload =
        }catch (Exception e){
            System.out.println(e.getMessage());
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
