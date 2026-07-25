package com.venky.demos.kstream.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.resource.data.topic:material-data}")
    private String dataTopic;

    @Value("${spring.resource.error.topic:material-error}")
    private String errorTopic;

    @Value("${spring.resource.observability.error.topic:common.observability.error}")
    private String observabilityErrorTopic;

    /**
     * Dispatches successfully transformed and schema-validated materials to the data topic.
     */
    public void sendToDataTopic(String key, String value) {
        logger.info("Sending message to data topic: {}", dataTopic);

        ProducerRecord<String, String> record = new ProducerRecord<>(
                dataTopic,
                key != null ? key : "",
                value
        );

        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                logger.error("Failed to send message to data topic {}: {}", dataTopic, ex.getMessage(), ex);
            } else {
                logger.info("Message sent successfully to data topic {} partition {} offset {}",
                        dataTopic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    /**
     * Redirects failed events to the error topic, preserving the raw poison payload in the record headers.
     */
    public void sendToErrorTopic(String key, String originalValue, String enrichedValue) {
        logger.warn("Routing processing failure to error topic: {}", errorTopic);

        ProducerRecord<String, String> errorRecord = new ProducerRecord<>(
                errorTopic,
                key != null ? key : "",
                enrichedValue
        );

        // Inject original poison payload text as bytes into the Kafka metadata headers for debugging
        errorRecord.headers().add(
                key != null ? key : "error",
                originalValue != null ? originalValue.getBytes() : "null".getBytes()
        );

        kafkaTemplate.send(errorRecord).whenComplete((result, ex) -> {
            if (ex != null) {
                logger.error("Failed to send message to error topic {}: {}", errorTopic, ex.getMessage(), ex);
            } else {
                logger.info("Error message sent successfully to error topic {} partition {} offset {}",
                        errorTopic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    /**
     * Critical structural fallback handler used if the standard error topic routing itself crashes.
     */
    public void sendToErrorTopicFallback(String key, String value) {
        logger.error("Fallback path triggered! Sending directly to observability sink: {}", observabilityErrorTopic);

        ProducerRecord<String, String> errorRecord = new ProducerRecord<>(observabilityErrorTopic, value);

        kafkaTemplate.send(errorRecord);
        logger.warn("Fallback: error message sent to observability error topic: {}", observabilityErrorTopic);
    }
}
