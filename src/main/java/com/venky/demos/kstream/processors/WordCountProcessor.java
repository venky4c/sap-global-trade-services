//package com.venky.demos.kstream.processors;
//
//import com.venky.demos.kstream.config.KafkaStreamsProperties;
//import jakarta.annotation.PostConstruct;
//import jakarta.annotation.PreDestroy;
//import lombok.RequiredArgsConstructor;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.KeyValue;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.Topology;
//import org.apache.kafka.streams.kstream.KStream;
//import org.apache.kafka.streams.kstream.Materialized;
//import org.apache.kafka.streams.kstream.Repartitioned;
//import org.apache.kafka.streams.state.Stores;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.stereotype.Component;
//
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.Properties;
//import java.util.regex.Pattern;
//
//@Component
//@RequiredArgsConstructor
//public class WordCountProcessor {
//
//    private final Logger log = LoggerFactory.getLogger(WordCountProcessor.class.getSimpleName());
//    private final KafkaStreamsProperties kafkaStreamsProperties;
//    private KafkaStreams streams;
//
//    @PostConstruct
//    public void startKafkaStreams() {
//        // 1. Get the configuration cluster credentials
//        Properties props = kafkaStreamsProperties.asProperties();
//        log.info("Props are {}", props);
//
//        // 2. Use the StreamsBuilder to design the blueprint steps
//        StreamsBuilder builder = new StreamsBuilder();
//        KStream<String, String> source = builder.stream("word-count-input");
//
//        final Pattern pattern = Pattern.compile("\\W+");
//
//        KStream<String, String> counts = source
//                .peek((key, value) -> log.info("[INCOMING] Key: {}, Value: {}", key, value))
//                .flatMapValues(value -> value == null ?
//                        Collections.emptyList() : Arrays.asList(pattern.split(value.toLowerCase())))
//                .map((key, value) -> new KeyValue<>(value, value))
//                .filter((key, value) -> !value.equals("the"))
//
//       // Why This Step Is Highly Recommended:Predictable Topic Names: This overrides the default identifier name, forcing the engine to name your
//                // state store backup topic exactly: word-count-input-word-count-store-changelog.Easy Pre-Creation:
//                // If Aiven throws a PolicyViolationException on the changelog topic during start-up, you will know the exact name
//                // needed to pre-create it manually in your Aiven Web Console.
//
//
//// ... Inside your pipeline layout builder:
//
//                .repartition(Repartitioned.as("word-count-repartition"))
//
//                .groupByKey()
//
//                // FIX: Force Kafka Streams to use in-memory storage WITHOUT a backup changelog topic.
//                // To completely stop Kafka Streams from creating this changelog topic and bypass Aiven's 5-topic limit,
//                // change your state storage engine from RocksDB (which requires a persistent backup topic) to a lightweight In-Memory architecture.
//                // Update the .count() section of your pipeline inside WordCountProcessor.java to use an in-memory store provider:
//                .count(Materialized.<String, Long>as(
//                                Stores.inMemoryKeyValueStore("word-count-store")) // Uses local RAM instead of RocksDB
//                        .withKeySerde(Serdes.String())
//                        .withValueSerde(Serdes.Long())
//                        .withLoggingDisabled()) // CRITICAL: Disables the creation of the backup changelog topic!
//
//                .mapValues(Object::toString)
//                .toStream()
//
//                .peek((word, count) -> log.info("[OUTBOUND] Word: '{}' -> Count: {}", word, count));
//
//        counts.to("word-count-output");
//
//        // 3. EXPLICIT: Compile the blueprint builder into a physical Topology object
//        Topology topology = builder.build();
//
//        // This log will print the exact graph structure of your processors in the terminal console!
//        log.info("--- VISUALIZING THE KAFKA STREAMS TOPOLOGY GRAPH ---");
//        log.info("\n{}", topology.describe());
//        log.info("-----------------------------------------------------");
//
//        // 4. Pass the exact compiled blueprint along with configurations to the execution runner
//        streams = new KafkaStreams(topology, props);
//
//        log.info("Starting Kafka Streams Client...");
//        streams.start();
//    }
//
//    @PreDestroy
//    public void shutdownKafkaStreams() {
//        if (streams != null) {
//            log.info("Shutting down Kafka Streams Client...");
//            streams.close();
//        }
//    }
//}