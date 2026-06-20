package com.example.cms_adapter.dto;

import java.io.Serializable;

public class OrderDTO implements Serializable {
    private String orderId;
    private String clientId;
    private String address;

    public OrderDTO(){}

    public OrderDTO(String orderId, String clientId, String address) {
        this.orderId = orderId;
        this.clientId = clientId;
        this.address = address;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getClientId() {
        return clientId;
    }

    public String getAddress() {
        return address;
    }
}

