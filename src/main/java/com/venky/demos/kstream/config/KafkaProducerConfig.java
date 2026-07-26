package com.venky.demos.kstream.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    private final KafkaStreamsProperties streamsProperties;

    public KafkaProducerConfig(KafkaStreamsProperties streamsProperties) {
        this.streamsProperties = streamsProperties;
    }

    /**
     * Instantiates a generic ProducerFactory with matching Aiven SASL credentials
     * for standard, non-streaming microservice components.
     */
    @Bean(name = "standardProducerFactory")
    public ProducerFactory<String, String> standardProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, streamsProperties.getBootstrapServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, streamsProperties.getSecurityProtocol());
        configProps.put(SaslConfigs.SASL_MECHANISM, streamsProperties.getSaslMechanism());

        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        configProps.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(jaasTemplate, streamsProperties.getSaslUsername(), streamsProperties.getSaslPassword()));

        //Inject your secure truststore configuration parameters explicitly here
        configProps.put("ssl.truststore.location", streamsProperties.getTruststoreLocation());
        configProps.put("ssl.truststore.type", "jks");
        configProps.put("ssl.truststore.password", streamsProperties.getSslTruststorePassword());
        configProps.put("ssl.endpoint.identification.algorithm", streamsProperties.getSslEndpointIdentificationAlgorithm());

        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Primary operational template exposed to background services to interact
     * directly with target topics using standard producers.
     */
    @Bean(name = "standardKafkaTemplate")
    public KafkaTemplate<String, String> standardKafkaTemplate() {
        return new KafkaTemplate<>(standardProducerFactory());
    }
}