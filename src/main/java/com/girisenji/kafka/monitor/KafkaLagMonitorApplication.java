package com.girisenji.kafka.monitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaLagMonitorApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaLagMonitorApplication.class, args);
    }
}
