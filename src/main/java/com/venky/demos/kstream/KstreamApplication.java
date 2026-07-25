package com.venky.demos.kstream;

import com.venky.demos.kstream.config.KafkaStreamsProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

// FIX: Exclude by string literal name to bypass missing class path import errors completely
@SpringBootApplication(excludeName = {
        "org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration",
        "org.springframework.boot.autoconfigure.kafka.KafkaStreamsAutoConfiguration"
})
@EnableConfigurationProperties(KafkaStreamsProperties.class)
public class KstreamApplication {

    private static final Logger log = LoggerFactory.getLogger(KstreamApplication.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Starting up the SAP GTS Material Kafka Streams Processing Node...");
        SpringApplication.run(KstreamApplication.class, args);
        log.info("System Initialization Sequence Completed Successfully.");
    }
}