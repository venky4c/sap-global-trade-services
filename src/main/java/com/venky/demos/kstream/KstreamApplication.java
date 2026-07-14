package com.venky.demos.kstream;

import com.venky.demos.kstream.config.KafkaStreamsProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication(excludeName = {
        "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration",
        "org.springframework.boot.autoconfigure.kafka.KafkaStreamsAutoConfiguration"
})
@EnableConfigurationProperties(KafkaStreamsProperties.class)
public class KstreamApplication {

    private static final Logger log = LoggerFactory.getLogger(KstreamApplication.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Inside KstreamApplication >>>>>>>");
        SpringApplication.run(KstreamApplication.class, args);
    }
}