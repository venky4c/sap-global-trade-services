package com.venky.demos.kstream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class CustomDeserializationExceptionHandler implements DeserializationExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(CustomDeserializationExceptionHandler.class);

    private KafkaProducer<byte[], byte[]> dlqProducer;
    private String dlqTopic = "material-error";

    @Override
    public void configure(Map<String, ?> configs) {
        logger.info("Initializing Custom Deserialization Exception Handler with cluster security profiles...");
        Properties props = new Properties();

        // Extract parameters safely by converting the generic values directly to Strings
        String bootstrapServers = configs.containsKey("bootstrap.servers") ? configs.get("bootstrap.servers").toString() : null;
        if (bootstrapServers == null) {
            bootstrapServers = "kafka-962173a-venky4c-74a1.j.aivencloud.com:21341";
        }

        String securityProtocol = configs.containsKey("security.protocol") ? configs.get("security.protocol").toString() : "SASL_SSL";
        String saslMechanism = configs.containsKey("sasl.mechanism") ? configs.get("sasl.mechanism").toString() : "SCRAM-SHA-256";

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);

        // Extract the structural JAAS config string safely
        String jaasConfig = configs.containsKey("sasl.jaas.config") ? configs.get("sasl.jaas.config").toString() : null;
        if (jaasConfig == null) {
            String username = "avnadmin";
            String password = System.getenv("KAFKA_SASL_PASSWORD");
            if (password == null || password.isBlank()) {
                throw new IllegalStateException(
                        "CRITICAL SECURITY ERROR: The environment variable 'KAFKA_SASL_PASSWORD' is not set. " +
                                "To protect your cluster credentials, hardcoded passwords are not allowed."
                );
            }
            jaasConfig = String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";", username, password);
        }
        props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

        // Forward Truststore files cleanly onto the underlying inner network handler
        if (configs.containsKey("ssl.truststore.location")) {
            props.put("ssl.truststore.location", configs.get("ssl.truststore.location").toString());
            props.put("ssl.truststore.type", configs.containsKey("ssl.truststore.type") ? configs.get("ssl.truststore.type").toString() : "jks");
            props.put("ssl.truststore.password", configs.get("ssl.truststore.password").toString());
        } else {
            props.put("ssl.truststore.location", "D:/forGit/kafka/global-trade-service/kstream/src/main/resources/client.truststore.jks");
            props.put("ssl.truststore.type", "jks");
            props.put("ssl.truststore.password", System.getenv("KAFKA_TRUSTSTORE_PASSWORD"));
        }

        String endpointAlgo = configs.containsKey("ssl.endpoint.identification.algorithm") ? configs.get("ssl.endpoint.identification.algorithm").toString() : "";
        props.put("ssl.endpoint.identification.algorithm", endpointAlgo);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        try {
            this.dlqProducer = new KafkaProducer<>(props);
            logger.info("Raw byte DLQ Exception safety handler compiled and active.");
        } catch (Exception e) {
            logger.error("Failed to initialize standalone raw DLQ Producer link", e);
        }
    }

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context,
                                                 ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {
        logger.error("Exception caught during raw byte Deserialization! Sending copy to dead letter topic: {}", dlqTopic, exception);

        if (dlqProducer != null) {
            try {
                dlqProducer.send(new ProducerRecord<>(dlqTopic, record.key(), record.value())).get();
            } catch (Exception ex) {
                logger.error("Failed to publish poison bytes out to validation fallback topics", ex);
            }
        }

        return DeserializationHandlerResponse.CONTINUE;
    }
}