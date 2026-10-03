package com.vijendra.pulsestream.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class PulseOrderApplication {
    public static void main(String[] args) {
        SpringApplication.run(PulseOrderApplication.class, args);
    }
}
