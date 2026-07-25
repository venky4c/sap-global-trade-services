package com.venky.demos.kstream.service;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;

/*
This service provides the tracking engine with methods to explicitly modify event frames depending on execution outcomes,
ensuring that downstream monitoring dashboards catch transaction states cleanly
*/

@Service
public class ObservabilityEnrichmentService {
    private static final Logger logger = LoggerFactory.getLogger(ObservabilityEnrichmentService.class);

    private static final String EVENT_KEY = "event";
    private static final String OBSERVABILITY_KEY = "observability";
    private static final String OBSERVABILITY_HEADER_KEY = "observabilityHeader";
    private static final String OBSERVABILITY_LINE_KEY = "observabilityLine";
    private static final String OBSERVABILITY_ERROR_KEY = "observabilityError";

    private static final String REPORTING_SYSTEM_ID = "CONFLUENT-KAFKA";
    private static final String REPORTING_SYSTEM_INSTANCE = "";
    private static final String STAGE_CODE_SUCCESS = "INT-COMPLETE";
    private static final String STAGE_CODE_ERROR = "INT-ERROR";
    private static final String STATUS_INPROGRESS = "INPROGRESS";
    private static final String STATUS_ERROR = "ERROR";
    private static final String REPORTING_SYSTEM_TYPE_INT = "INT";

    @Value("${spring.resource.environment:DEV}")
    private String currentEnvironment;

    @Value("${spring.resource.observability.enrich-error:false}")
    private boolean enrichObservabilityErrorEnabled;

    /**
     * Updates tracing context blocks to signal a fully successful data translation step.
     */
    public void enrichForSuccess(JSONObject valueObject, String topicName) {
        logger.debug("Enriching traceability context block with success state for topic: {}", topicName);
        JSONObject observabilityObject = getOrCreateObservabilityObject(valueObject);
        enrichObservabilityHeader(valueObject, observabilityObject);
        enrichObservabilityLineForSuccess(observabilityObject, topicName);
    }

    /**
     * Injects descriptive failure maps into metadata tracking objects for debugging pipelines.
     */
    public void enrichForError(JSONObject valueObject, Exception exception, String topicName) {
        logger.warn("Enriching traceability context block with error state for topic: {}", topicName);
        JSONObject observabilityObject = getOrCreateObservabilityObject(valueObject);
        enrichObservabilityHeader(valueObject, observabilityObject);

        ErrorDetails errorDetails = determineErrorDetails(exception);
        enrichObservabilityLineForError(observabilityObject, errorDetails, topicName);

        if (enrichObservabilityErrorEnabled) {
            enrichObservabilityError(observabilityObject, errorDetails);
        }
    }

    private JSONObject getOrCreateObservabilityObject(JSONObject root) {
        JSONObject eventObj = root.has(EVENT_KEY) ? root.getJSONObject(EVENT_KEY) : new JSONObject();
        if (!root.has(EVENT_KEY)) {
            root.put(EVENT_KEY, eventObj);
        }

        JSONObject obsObj = eventObj.has(OBSERVABILITY_KEY) ? eventObj.getJSONObject(OBSERVABILITY_KEY) : new JSONObject();
        if (!eventObj.has(OBSERVABILITY_KEY)) {
            eventObj.put(OBSERVABILITY_KEY, obsObj);
        }
        return obsObj;
    }

    private void enrichObservabilityHeader(JSONObject root, JSONObject observability) {
        JSONObject header = observability.has(OBSERVABILITY_HEADER_KEY)
                ? observability.getJSONObject(OBSERVABILITY_HEADER_KEY)
                : new JSONObject();

        String transactionId = root.has(EVENT_KEY) ? root.getJSONObject(EVENT_KEY).optString("transactionId", "UNKNOWN") : "UNKNOWN";

        header.put("dataObjectName", "MATERIAL");
        header.put("documentId", transactionId);
        header.put("locationId", "");
        header.put("transactionId", transactionId);
        header.put("modifiedDate", Instant.now().toString());

        observability.put(OBSERVABILITY_HEADER_KEY, header);
    }

    private void enrichObservabilityLineForSuccess(JSONObject observability, String topicName) {
        JSONObject line = observability.has(OBSERVABILITY_LINE_KEY) ? observability.getJSONObject(OBSERVABILITY_LINE_KEY) : new JSONObject();

        line.put("executionDate", Instant.now().toString());
        line.put("status", STATUS_INPROGRESS);
        line.put("stageCode", STAGE_CODE_SUCCESS);
        line.put("componentName", "mdm.sap-gts.material.kstream");
        line.put("reportingSystemEnvironment", currentEnvironment);
        line.put("reportingSystemId", REPORTING_SYSTEM_ID);
        line.put("reportingSystemInstance", REPORTING_SYSTEM_INSTANCE);
        line.put("reportingSystemType", REPORTING_SYSTEM_TYPE_INT);

        observability.put(OBSERVABILITY_LINE_KEY, line);
    }

    private void enrichObservabilityLineForError(JSONObject observability, ErrorDetails details, String topicName) {
        JSONObject line = observability.has(OBSERVABILITY_LINE_KEY) ? observability.getJSONObject(OBSERVABILITY_LINE_KEY) : new JSONObject();

        line.put("executionDate", Instant.now().toString());
        line.put("status", STATUS_ERROR);
        line.put("stageCode", STAGE_CODE_ERROR);
        line.put("componentName", "mdm.sap-gts.material.kstream");
        line.put("reportingSystemEnvironment", currentEnvironment);
        line.put("reportingSystemId", REPORTING_SYSTEM_ID);
        line.put("errorType", details.errorType);
        line.put("errorCode", details.errorCode);
        line.put("errorDescription", details.errorDescription);

        observability.put(OBSERVABILITY_LINE_KEY, line);
    }

    private void enrichObservabilityError(JSONObject observability, ErrorDetails details) {
        JSONObject errorObj = observability.has(OBSERVABILITY_ERROR_KEY) ? observability.getJSONObject(OBSERVABILITY_ERROR_KEY) : new JSONObject();

        errorObj.put("executionDate", Instant.now().toString());
        errorObj.put("errorType", details.errorType);
        errorObj.put("errorCode", details.errorCode);
        errorObj.put("errorDescription", details.errorDescription);
        errorObj.put("reportingSystemEnvironment", currentEnvironment);
        errorObj.put("reportingSystemId", REPORTING_SYSTEM_ID);

        observability.put(OBSERVABILITY_ERROR_KEY, errorObj);
    }

    private ErrorDetails determineErrorDetails(Exception exception) {
        String msg = exception.getMessage() != null ? exception.getMessage() : "Execution exception runtime fault.";
        if (exception instanceof RuntimeException && msg.contains("Schema")) {
            return new ErrorDetails("VALIDATION_ERROR", "ERR_VAL_400", msg);
        }
        return new ErrorDetails("TRANSLATION_ERROR", "ERR_SYS_500", msg);
    }

    private static class ErrorDetails {
        final String errorType;
        final String errorCode;
        final String errorDescription;

        ErrorDetails(String errorType, String errorCode, String errorDescription) {
            this.errorType = errorType;
            this.errorCode = errorCode;
            this.errorDescription = errorDescription;
        }
    }
}