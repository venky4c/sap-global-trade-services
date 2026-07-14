//package com.venky.demos.kstream.config;
//
//import lombok.Data;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.boot.context.properties.ConfigurationProperties;
//import org.springframework.context.annotation.Configuration;
//
//import java.util.Properties;
//
//@Data
//@Configuration
//@ConfigurationProperties(prefix = "kafka.streams")
//public class KafkaStreamsProperties {
//
//    private final Logger log = LoggerFactory.getLogger(KafkaStreamsProperties.class.getSimpleName());
//
//    private String applicationId;
//    private String bootstrapServers;
//    private String defaultKeySerde;
//    private String defaultValueSerde;
//
//    public Properties asProperties() {
//        log.info("KafkaStreamsProperties - applicationId: {}", applicationId);
//        log.info("KafkaStreamsProperties - bootstrapServers: {}", bootstrapServers);
//        log.info("KafkaStreamsProperties - defaultKeySerde: {}", defaultKeySerde);
//        log.info("KafkaStreamsProperties - defaultValueSerde: {}", defaultValueSerde);
//
//        Properties props = new Properties();
//        // Corrected keys to use dot notation
//        props.put("application.id", applicationId);
//        props.put("bootstrap.servers", bootstrapServers);
//        props.put("default.key.serde", defaultKeySerde);
//        props.put("default.value.serde", defaultValueSerde);
//        log.info("Props from KafkaStreamsProperties are {}", props);
//        return props;
//    }
//}

package com.venky.demos.kstream.config;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka.streams")
public class KafkaStreamsProperties {

    private final Logger log = LoggerFactory.getLogger(KafkaStreamsProperties.class.getSimpleName());

    private String applicationId;
    private String bootstrapServers;
    private String defaultKeySerde;
    private String defaultValueSerde;

    // New fields for security properties
    private String securityProtocol;
    private String saslMechanism;
    private String sslEndpointIdentificationAlgorithm;
    private String sslTruststoreType;
    private String truststoreLocation;
    private String sslTruststorePassword; // This will be populated from env var
    private String saslJaasConfig; // This will be populated from env var

    // Producer-specific properties (optional, if you need them in this bean)
    private String keySerializer;
    private String valueSerializer;
    private String saslUsername;
    private String saslPassword;


    public Properties asProperties() {
        log.info("KafkaStreamsProperties - applicationId: {}", applicationId);
        log.info("KafkaStreamsProperties - bootstrapServers: {}", bootstrapServers);
        log.info("KafkaStreamsProperties - defaultKeySerde: {}", defaultKeySerde);
        log.info("KafkaStreamsProperties - defaultValueSerde: {}", defaultValueSerde);
        log.info("KafkaStreamsProperties - securityProtocol: {}", securityProtocol);
        log.info("KafkaStreamsProperties - saslMechanism: {}", saslMechanism);
        log.info("KafkaStreamsProperties - sslEndpointIdentificationAlgorithm: {}", sslEndpointIdentificationAlgorithm);
        log.info("KafkaStreamsProperties - sslTruststoreType: {}", sslTruststoreType);
        log.info("KafkaStreamsProperties - truststoreLocation: {}", truststoreLocation);
        log.info("KafkaStreamsProperties - sslTruststorePassword: {}", sslTruststorePassword);
        //log.info("KafkaStreamsProperties - saslJaasConfig: {}", saslJaasConfig);
        log.info("KafkaStreamsProperties - keySerializer: {}", keySerializer);
        log.info("KafkaStreamsProperties - valueSerializer: {}", valueSerializer);

        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        saslJaasConfig = String.format(jaasTemplate, saslUsername, saslPassword);
        Properties props = new Properties();
        props.put("application.id", applicationId);
        props.put("bootstrap.servers", bootstrapServers);
        props.put("default.key.serde", defaultKeySerde);
        props.put("default.value.serde", defaultValueSerde);

        // Add security properties to the Kafka Properties object
        if (securityProtocol != null) props.put("security.protocol", securityProtocol);
        if (saslMechanism != null) props.put("sasl.mechanism", saslMechanism);
        if (sslEndpointIdentificationAlgorithm != null) props.put("ssl.endpoint.identification.algorithm", sslEndpointIdentificationAlgorithm);
        if (sslTruststoreType != null) props.put("ssl.truststore.type", sslTruststoreType);
        if (truststoreLocation != null) props.put("ssl.truststore.location", truststoreLocation);
        if (sslTruststorePassword != null) props.put("ssl.truststore.password", sslTruststorePassword);
        if (saslJaasConfig != null) props.put("sasl.jaas.config", saslJaasConfig);

        // Add producer properties if needed (though typically not for StreamsConfig directly)
        if (keySerializer != null) props.put("key.serializer", keySerializer);
        if (valueSerializer != null) props.put("value.serializer", valueSerializer);
        //props.put("topic.retention.ms", "604800000");

        log.info("Props from KafkaStreamsProperties are {}", props);
        return props;
    }
}