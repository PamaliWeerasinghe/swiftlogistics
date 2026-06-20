package com.example.order_processing.listener;

import com.example.order_processing.dto.OrderDTO;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class OrderProcessListener {

    private static final String order_intake_queue = "order.intake.queue";
    private static final String cms_adapter_queue = "cms.adapter.queue";

    private final RabbitTemplate rabbitTemplate;

    @Autowired
    public OrderProcessListener(RabbitTemplate rabbitTemplate){
        this.rabbitTemplate = rabbitTemplate;
    }

    @RabbitListener(queues = order_intake_queue)
    public void orderListener(OrderDTO orderDTO, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException{
        System.out.println("\n [ORDER LISTENER] Grabbed Order: "+ orderDTO.getOrderId());
        try {
            //Send to CMS Adapter
            rabbitTemplate.convertAndSend(cms_adapter_queue,orderDTO);
            channel.basicAck(tag, false);
            System.out.println("Handed over to cms.adapter.queue");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

}
