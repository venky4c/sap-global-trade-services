package com.venky.demos.kstream.controller;

import com.venky.demos.kstream.config.KafkaStreamsProperties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;
import java.util.UUID;

@RestController
@RequestMapping("/api/kafka")
public class KafkaJsonController {
    private static final Logger logger = LoggerFactory.getLogger(KafkaJsonController.class);

    private final KafkaStreamsProperties streamsProperties;
    private KafkaProducer<String, String> producer;

    @Value("${spring.resource.helper.topic:material-helper}")
    private String helperTopic;

    public KafkaJsonController(KafkaStreamsProperties streamsProperties) {
        this.streamsProperties = streamsProperties;
    }

    /**
     * Lazy initializer for the internal core Kafka Producer client instance.
     * Inherits the same SASL/SCRAM security credentials used by your streaming node.
     */
    private synchronized KafkaProducer<String, String> getProducer() {
        if (producer == null) {
            logger.info("Initializing lazy Kafka producer with bootstrap servers: {}", streamsProperties.getBootstrapServers());
            Properties props = new Properties();

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, streamsProperties.getBootstrapServers());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // Reliability & transactional guarantees matching page 8 parameters
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

            // Handle Secure Aiven SASL Connection Parameters Dynamically
            if (streamsProperties.getSecurityProtocol() != null &&
                    streamsProperties.getSecurityProtocol().toUpperCase().contains("SASL")) {

                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, streamsProperties.getSecurityProtocol());
                props.put(SaslConfigs.SASL_MECHANISM, streamsProperties.getSaslMechanism());

                String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
                props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(jaasTemplate, streamsProperties.getSaslUsername(), streamsProperties.getSaslPassword()));

                props.put("ssl.truststore.location", streamsProperties.getTruststoreLocation());
                props.put("ssl.truststore.type", "jks");

                // 🔒 CRITICAL SECURITY FIX: Extract the truststore password dynamically from env
                String truststorePassword = System.getenv("KAFKA_TRUSTSTORE_PASSWORD");
                if (truststorePassword == null || truststorePassword.isBlank()) {
                    // Fallback to the Spring bean parameter mapped from application.yml
                    truststorePassword = streamsProperties.getSslTruststorePassword();
                }
                props.put("ssl.truststore.password", truststorePassword);

                props.put("ssl.endpoint.identification.algorithm", streamsProperties.getSslEndpointIdentificationAlgorithm());

                logger.info("Kafka publisher endpoint configured with secure SASL/SSL credentials.");
            } else {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
                logger.info("Kafka publisher endpoint configured with local PLAINTEXT network protocol.");
            }

            producer = new KafkaProducer<>(props);
            logger.info("Kafka standalone test producer instantiated successfully.");
        }
        return producer;
    }

    /**
     * Public testing endpoint to push raw mock SAP material master data directly into the stream pipeline.
     */
    @PostMapping("/publish")
    public ResponseEntity<String> publishMessage(@RequestBody String jsonPayload) {
        logger.info("Received external web request to publish raw material data payload to cluster.");
        logger.info("Target landing destination topic: {}", helperTopic);

        String messageKey = generateKey();
        logger.info("Generated partition transaction tracking key: {}", messageKey);
        logger.debug("Raw request body payload content: {}", jsonPayload);

        ProducerRecord<String, String> record = new ProducerRecord<>(helperTopic, messageKey, jsonPayload);

        // Async dispatch out to your Aiven cluster brokers with logging callbacks
        getProducer().send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to append test record onto target log brokers!", exception);
            } else {
                logger.info("Event committed to broker partitions successfully. Topic={}, Partition={}, Offset={}, TrackingKey={}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        messageKey
                );
            }
        });

        return ResponseEntity.ok("JSON message securely forwarded onto active helper input topic: " + helperTopic);
    }

    private String generateKey() {
        return "helper-event-" + UUID.randomUUID();
    }
}