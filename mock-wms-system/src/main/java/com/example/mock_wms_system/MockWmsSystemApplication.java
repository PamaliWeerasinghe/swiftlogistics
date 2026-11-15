package com.example.mock_wms_system;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.example.mock_wms", "com.example.mock_wms_system"})
public class MockWmsSystemApplication {

	public static void main(String[] args) {
		SpringApplication.run(MockWmsSystemApplication.class, args);
	}

}
