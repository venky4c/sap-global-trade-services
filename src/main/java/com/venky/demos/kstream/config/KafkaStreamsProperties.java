package com.venky.demos.kstream.config;

import lombok.Data;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;
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
    private String sslTruststorePassword;
    private String saslUsername;
    private String saslPassword;

    /**
     * Reusable, DRY single source of truth for base network connectivity maps.
     */
    public Map<String, Object> buildCommonKafkaProperties() {
        Map<String, Object> commonProps = new HashMap<>();
        commonProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        commonProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, this.securityProtocol);

        if ("SASL_SSL".equalsIgnoreCase(this.securityProtocol)) {
            commonProps.put(SaslConfigs.SASL_MECHANISM, this.saslMechanism);
            commonProps.put("ssl.endpoint.identification.algorithm", this.sslEndpointIdentificationAlgorithm);
            commonProps.put("ssl.truststore.location", this.truststoreLocation);
            commonProps.put("ssl.truststore.type", "jks");
            commonProps.put("ssl.truststore.password", this.sslTruststorePassword);

            String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
            commonProps.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(jaasTemplate, this.saslUsername, this.saslPassword));
        }
        return commonProps;
    }

    /**
     * Compiles streams-specific parameters onto the shared base configurations.
     */
    public Properties asProperties() {
        Properties props = new Properties();
        // Inherit all base network and security configurations instantly
        props.putAll(buildCommonKafkaProperties());

        props.put("application.id", this.applicationId);
        props.put("default.key.serde", this.defaultKeySerde);
        props.put("default.value.serde", this.defaultValueSerde);
        props.put("default.deserialization.exception.handler", "com.venky.demos.kstream.CustomDeserializationExceptionHandler");

        return props;
    }
}