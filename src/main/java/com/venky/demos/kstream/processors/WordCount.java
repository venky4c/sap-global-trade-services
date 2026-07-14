package com.venky.demos.kstream.processors;

import com.venky.demos.kstream.config.KafkaStreamsProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

// FIX: Exclude by absolute string names to bypass package visibility shifts in Boot 4.x
@SpringBootApplication(excludeName = {
        "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration",
        "org.springframework.boot.autoconfigure.kafka.KafkaStreamsAutoConfiguration"
})
@EnableConfigurationProperties(KafkaStreamsProperties.class)
public class WordCount {

    public static void main(String[] args) {
        SpringApplication.run(WordCount.class, args);
    }
}