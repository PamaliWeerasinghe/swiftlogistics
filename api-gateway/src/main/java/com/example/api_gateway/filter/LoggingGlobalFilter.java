package com.example.api_gateway.filter;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.URI;

@Component
public class LoggingGlobalFilter implements GlobalFilter, Ordered {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        URI requestUri = exchange.getRequest().getURI();
        String method = exchange.getRequest().getMethod().name();

        System.out.println("🌐 [API GATEWAY :8000] Incoming " + method + " -> " + requestUri.getPath());

        return chain.filter(exchange).then(Mono.fromRunnable(() -> {
            int statusCode = exchange.getResponse().getStatusCode() != null
                    ? exchange.getResponse().getStatusCode().value()
                    : 200;
            System.out.println("   ✅ [API GATEWAY :8000] Completed " + requestUri.getPath() + " with HTTP " + statusCode);
        }));
    }

    @Override
    public int getOrder() {
        return -1; // Execute before routing filters
    }
}