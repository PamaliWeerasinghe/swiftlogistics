package com.example.cms.Controller;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RestController
// Allow your frontend to talk to this backend if they run on different configurations
@CrossOrigin(origins = "*")
public class MockCmsController {

    // Simulates an internal legacy database table
    private final List<Map<String, String>> contractDatabase = new ArrayList<>();

    // Feature flag to simulate system outages
    private boolean isSystemOnline = true;

    // 1. SOAP ENDPOINT (Called by your CMS Adapter)
    @PostMapping(
            value = "/mock-cms/services/OrderService",
            consumes = MediaType.TEXT_XML_VALUE,
            produces = MediaType.TEXT_XML_VALUE
    )
    public ResponseEntity<String> handleSoapOrderRequest(@RequestBody String requestXml) {
        if (!isSystemOnline) {
            return ResponseEntity.status(503).body("<soapenv:Fault><faultstring>Legacy CMS Mainframe Offline</faultstring></soapenv:Fault>");
        }

        // Extract values using simple string slicing
        String orderId = extractTagValue(requestXml, "ord:OrderId");
        String clientId = extractTagValue(requestXml, "ord:ClientId");
        String address = extractTagValue(requestXml, "ord:Address");

        // Save into our mock database array list
        Map<String, String> record = new ConcurrentHashMap<>();
        record.put("orderId", orderId);
        record.put("clientId", clientId);
        record.put("address", address);
        record.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        contractDatabase.add(0, record); // Keep newest records at top

        // Build SOAP Response
        String soapResponseXml =
                "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ord=\"http://example.com/order\">\n" +
                        "   <soapenv:Header/>\n" +
                        "   <soapenv:Body>\n" +
                        "      <ord:OrderResponse>\n" +
                        "         <ord:Status>SUCCESS</ord:Status>\n" +
                        "         <ord:Message>Contract registered and ledger billed successfully.</ord:Message>\n" +
                        "         <ord:ProcessedOrderId>" + orderId + "</ord:ProcessedOrderId>\n" +
                        "      </ord:OrderResponse>\n" +
                        "   </soapenv:Body>\n" +
                        "</soapenv:Envelope>";

        return ResponseEntity.ok(soapResponseXml);
    }

    // 2. FRONTEND REST ENDPOINTS (To populate your dashboard UI)
    @GetMapping("/api/mock-cms/contracts")
    public List<Map<String, String>> getContracts() {
        return contractDatabase;
    }

    @GetMapping("/api/mock-cms/status")
    public Map<String, Object> getStatus() {
        return Map.of("online", isSystemOnline, "totalRecords", contractDatabase.size());
    }

    @PostMapping("/api/mock-cms/toggle")
    public Map<String, Boolean> toggleStatus() {
        this.isSystemOnline = !this.isSystemOnline;
        return Map.of("online", isSystemOnline);
    }

    private String extractTagValue(String xml, String tagName) {
        try {
            int start = xml.indexOf("<" + tagName + ">") + ("<" + tagName + ">").length();
            int end = xml.indexOf("</" + tagName + ">");
            return xml.substring(start, end);
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }
}