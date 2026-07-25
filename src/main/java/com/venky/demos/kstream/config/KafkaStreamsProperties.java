package com.venky.demos.kstream.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.Properties;

@Data
@ConfigurationProperties(prefix = "kafka.streams")
public class KafkaStreamsProperties {

    private String applicationId;
    private String bootstrapServers;
    private String defaultKeySerde;
    private String defaultValueSerde;
    private String securityProtocol;
    private String saslMechanism;
    private String sslEndpointIdentificationAlgorithm;
    private String truststoreLocation;
    private String sslTruststorePassword; // Automatically mapped from application.yml
    private String saslUsername;
    private String saslPassword;         // Automatically mapped from application.yml

    public Properties asProperties() {
        Properties props = new Properties();
        props.put("application.id", this.applicationId);
        props.put("bootstrap.servers", this.bootstrapServers);
        props.put("default.key.serde", this.defaultKeySerde);
        props.put("default.value.serde", this.defaultValueSerde);
        props.put("security.protocol", this.securityProtocol);
        props.put("sasl.mechanism", this.saslMechanism);
        props.put("ssl.endpoint.identification.algorithm", this.sslEndpointIdentificationAlgorithm);
        props.put("ssl.truststore.location", this.truststoreLocation);
        props.put("ssl.truststore.type", "jks");

        // The mapped environment property is fetched here
        props.put("ssl.truststore.password", this.sslTruststorePassword);

        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        props.put("sasl.jaas.config", String.format(jaasTemplate, this.saslUsername, this.saslPassword));

        props.put("default.deserialization.exception.handler", "com.venky.demos.kstream.CustomDeserializationExceptionHandler");

        return props;
    }
}