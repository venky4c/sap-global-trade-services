package com.venky.demos.kstream.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaClientConfig {

    // Centralized single source of truth configuration bean
    private final KafkaStreamsProperties streamsProperties;

    /**
     * Configures the shared Aiven connection properties used by standard
     * out-of-band Kafka clients like the REST test controller and DLQ routers.
     */
    @Bean(name = "standardProducerFactory")
    public ProducerFactory<String, String> standardProducerFactory() {
        // Inherit the uniform base connectivity maps natively from your properties
        Map<String, Object> configProps = streamsProperties.buildCommonKafkaProperties();

        // Add robust, idempotent producer-specific quality settings
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Exposes the primary operational template used by your KafkaProducerService
     * to publish successful migrations and error dead-letter packets.
     */
    @Bean(name = "standardKafkaTemplate")
    public KafkaTemplate<String, String> standardKafkaTemplate() {
        return new KafkaTemplate<>(standardProducerFactory());
    }
}