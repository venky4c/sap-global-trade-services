package com.venky.demos.kstream.processors;

import com.venky.demos.kstream.config.KafkaStreamsProperties;
import com.venky.demos.kstream.service.LocalSchemaValidationService;
import com.venky.demos.kstream.service.MessageTransformationService;
import com.venky.demos.kstream.service.KafkaProducerService;
import com.venky.demos.kstream.util.NullReplacementUtil;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class SapGtsMaterialStreamProcessor {

    private final Logger log = LoggerFactory.getLogger(SapGtsMaterialStreamProcessor.class.getSimpleName());

    private final KafkaStreamsProperties kafkaStreamsProperties;
    private final MessageTransformationService messageTransformationService;
    private final LocalSchemaValidationService localSchemaValidationService;
    private final KafkaProducerService kafkaProducerService;

    @Value("${spring.resource.helper.topic:material-helper}")
    private String helperTopic;

    @Value("${spring.resource.data.topic:material-data}")
    private String dataTopic;

    private KafkaStreams streams;

    @PostConstruct
    public void startKafkaStreams() {
        Properties props = kafkaStreamsProperties.asProperties();
        log.info("Kafka Streams Configurations initialized successfully.");

        StreamsBuilder builder = new StreamsBuilder();

        // Listen to raw inbound material events drop from SAP GTS
        KStream<String, String> sourceStream = builder.stream(helperTopic);

        sourceStream.foreach((key, value) -> {
            String resolvedKey = (key != null && !key.isBlank()) ? key : "helper-event-" + UUID.randomUUID();
            log.info("Message received in helper topic. Processing Key: {}", resolvedKey);

            try {
                // 1. Sanitize incoming strings using standard null utility check strings
                if (value == null || value.isBlank()) {
                    throw new IllegalArgumentException("Payload value text cannot be empty or null.");
                }

                // 2. Parse text envelope string directly into JSON model nodes
                JSONObject rawJson = new JSONObject(value);

                // 3. Execute data transformation mappings
                JSONObject transformedJson = messageTransformationService.transformMessage(rawJson);
                String transformedValueStr = transformedJson.toString();

                // 4. Validate structures against your open-source Networknt schemas
                localSchemaValidationService.validateAndConvert(transformedValueStr, dataTopic);

                // 5. Success Path: Push the validated payload onto your data cluster topic
                kafkaProducerService.sendToDataTopic(resolvedKey, transformedValueStr);

            } catch (Exception ex) {
                log.warn("Data validation or processing error caught in stream thread loop: {}", ex.getMessage());

                // 6. Error Routing Path: Capture error data and push to your dedicated error dead-letter queue
                try {
                    // Enrich standard tracking log envelope with runtime validation messages
                    JSONObject errorEnvelope = new JSONObject();
                    errorEnvelope.put("errorDescription", ex.getMessage());
                    errorEnvelope.put("status", "ERROR");
                    errorEnvelope.put("stageCode", "INT-ERROR");

                    kafkaProducerService.sendToErrorTopic(resolvedKey, value, errorEnvelope.toString());
                } catch (Exception fatalEx) {
                    log.error("Fatal routing path exception crash. Triggering hard backup strategy sink", fatalEx);
                    kafkaProducerService.sendToErrorTopicFallback(resolvedKey, value);
                }
            }
        });

        // Compile stream definitions into physical graph structures
        Topology topology = builder.build();

        log.info("--- VISUALIZING THE MDM SAP GTS MATERIAL STREAM TOPOLOGY ---");
        log.info("\n{}", topology.describe());
        log.info("-------------------------------------------------------------");

        streams = new KafkaStreams(topology, props);
        log.info("Starting SAP GTS Master Data Management Material Processing Service...");
        streams.start();
    }

    @PreDestroy
    public void shutdownKafkaStreams() {
        if (streams != null) {
            log.info("Gracefully locking down active material client processing streams...");
            streams.close();
        }
    }
}
