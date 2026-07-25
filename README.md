
## 🚀 Enterprise SAP-GTS Data Migration Pipeline (Kafka Streams)

A battle-hardened, real-time data migration pipeline built to ingest complex, multi-nested SAP Material Master records, transform legacy dates/arrays into modern data formats, validate payload schemas natively, and handle edge-case failures without breaking stream topologies.
Built specifically to operate within cloud infrastructure constraints (such as a 5-topic ceiling on multi-tenant clusters), this project demonstrates an advanced architectural shift from traditional heavy, stateful streaming models to a lightweight, highly resilient stateless event-driven pattern.
------------------------------
## 🗺️ Architectural Concept Map
(🎨 [Placeholder: docs/assets/kafka-architecture.png] - Coming soon: Visual breakdown of Producers, Consumer Groups, and Partition Routing strategies).

[ SAP GTS Source ]
│
▼ (HTTPS / CPI)
┌───────────────────────┐
│  KafkaJsonController  │ ──► Publishes to raw data landing pad
└───────────────────────┘
│
▼
📥 Topic: material-helper  (Partitioned Raw Log)
│
├─── [ Valid Payload ] ──► Transform ──► Validate ──► 📤 Topic: material-data
│
└─── [ Poison Pill / Exception ] ────────────────────► ❌ Topic: material-error (DLQ)

------------------------------
## 🎯 Strategic Business Impact 

* Zero System Downtime: Custom deserialization safety layers ensure poison pills (corrupted bits/bad frames) are caught at the boundary, ensuring a single bad record never stalls global enterprise data sync loops.
* Cloud Resource Optimization: By shifting processing models to a stateless approach, this application operates completely free of local disk overhead (RocksDB), cutting cloud infrastructure costs while avoiding multi-tenant storage policies.
* Traceability and Observability: Injects standardized tracking blocks (stageCode, status) directly into data payloads, enabling real-time monitoring of multi-million record migration cycles via Datadog or ELK.

------------------------------
## 🛠️ Deep-Dive Technical Recipes 
This project serves as a concrete engineering blueprint for advanced Kafka and Spring patterns. Use the quick-links below to grab the exact implementation code.
## 1. Robust SASL/SCRAM Connection Blueprints
Connecting securely to managed cloud brokers (like Aiven) over SASL_SSL using encrypted credentials requires custom configuration properties to prevent hardcoded credential issues.

* Implementation Reference: KafkaStreamsProperties.java
* Configuration Blueprint: application.yml

## 2. Base Configurations & Dynamic Scenario Augmentation
To share a uniform base environment layout (Brokers, SSL protocols, JAAS login templates) across multiple modules while augmenting configurations for specific workloads (e.g., enabling strict Idempotency for high-value pipelines), use standalone producer and stream configuration factories.

* Streams Bean Mapping: KafkaStreamsConfig.java
* Augmented Standalone Producer Factory: KafkaProducerConfig.java (Enforces ENABLE_IDEMPOTENCE_CONFIG = true and ACKS_CONFIG = all for critical message delivery consistency).

## 3. Edge Safety Deserialization Handlers (Dead Letter Queues)
If a source system drops unparseable junk bytes (broken serialization strings) into your pipeline, Kafka Streams' default behavior is to panic and shut down the engine. This handler intercepts bad bytes right at the boundary, copies them onto an audit DLQ topic, and triggers a CONTINUE signal to keep the consumer group alive.

* Implementation Reference: CustomDeserializationExceptionHandler.java


## 4. Highly Resilient Stateless Processing Topologies
To prevent policy violations on multi-tenant brokers with low topic thresholds, this template avoids using intermediate stateful storage handlers (like RocksDB or state stores) by orchestrating complex transformation and error-handling pipelines inside a stateless execution loop.

* Core Stream Processor Logic: SapGtsMaterialStreamProcessor.java

------------------------------
## 📚 Core Kafka Core Concepts Implemented

* Consumer Group Coordination: Uses unique application.id strings to establish isolated consumer groups on your broker, automatically sharing partition ownership across multiple application instances for horizontal scale.
* Stateless vs Stateful Topologies: Eliminates changelog and repartitioning topics by utilizing inline processing functions, avoiding storage overhead and ensuring fast stream consumer rebalances.
* Dead Letter Queue (DLQ) Architecture: Implements a multi-layered DLQ strategy. It uses low-level byte-traps for corrupted schemas and high-level routing handlers for logical exceptions, ensuring complete preservation of data context during system anomalies.
* Idempotent Network Handlers: Configures producer factories with transactional sequence tracking parameters to guarantee exactly-once network deliveries, preventing duplicate records during network retries.

------------------------------
## ⚡ Quick Start Local Testing Sequence
## 1. Spin up Local Environment Credentials
Cloud credentials and local truststore certificates are mapped to the proper paths inside your local property file:

# Environment properties are properly secured via shell variables
# Verify variables are visible to the process
$env:KAFKA_TRUSTSTORE_PASSWORD
$env:KAFKA_SASL_PASSWORD


## 2. Compile and Verify the Project Code
Run a full verification and compilation build to ensure the data models, Lombok annotations, and open-source JSON validators are securely indexed:

./gradlew clean build -x test --no-daemon

## 3. Initiate the Stream Processing Application
Launch the application context locally on your laptop:

./gradlew bootRun

