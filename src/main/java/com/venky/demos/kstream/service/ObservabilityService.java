package com.venky.demos.kstream.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.venky.demos.kstream.model.ObservabilityHeader;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/*
This service handles the metadata insertion into your events. It dynamically wraps or extends the incoming message payload with
 real-time tracking frames (stageCode: SRC-COMPLETE, status: INPROGRESS) so the events can be monitored end-to-end as they pass from SAP
 through the stream nodes.
*/

@Service
public class ObservabilityService {
    private static final Logger logger = LoggerFactory.getLogger(ObservabilityService.class);

    private static final String EVENT_KEY = "event";
    private static final String OBSERVABILITY_KEY = "observability";
    private static final String OBSERVABILITY_HEADER_KEY = "observabilityHeader";
    private static final String OBSERVABILITY_LINE_KEY = "observabilityLine";
    private static final String OBSERVABILITY_ERROR_KEY = "observabilityError";

    private static final String STAGE_CODE = "SRC-COMPLETE";
    private static final String REPORTING_SYSTEM_ID = "CONFLUENT-KAFKA";
    private static final String REPORTING_SYSTEM_TYPE = "SRC";
    private static final String STATUS_INPROGRESS = "INPROGRESS";

    @Value("${spring.resource.environment:DEV}")
    private String currentEnvironment;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Injects tracking parameters into clean data records destined for downstream consumption.
     */
    public String enrichDataTopicMessage(String originalValue, JSONObject valueObject) throws JsonProcessingException {
        logger.debug("Enriching successful data message trace block");
        JSONObject enrichedObject = new JSONObject(originalValue);
        JSONObject observabilityObject = getOrCreateObservabilityObject(enrichedObject);

        JSONObject observabilityLine = observabilityObject.has(OBSERVABILITY_LINE_KEY)
                ? observabilityObject.getJSONObject(OBSERVABILITY_LINE_KEY)
                : new JSONObject();

        observabilityLine.put("stageCode", STAGE_CODE);
        observabilityLine.put("status", STATUS_INPROGRESS);
        observabilityLine.put("reportingSystemEnvironment", currentEnvironment);
        observabilityLine.put("reportingSystemId", REPORTING_SYSTEM_ID);
        observabilityLine.put("reportingSystemInstance", "");
        observabilityLine.put("reportingSystemType", REPORTING_SYSTEM_TYPE);

        observabilityObject.put(OBSERVABILITY_LINE_KEY, observabilityLine);

        if (observabilityObject.has(OBSERVABILITY_HEADER_KEY)) {
            JSONObject observabilityHeader = observabilityObject.getJSONObject(OBSERVABILITY_HEADER_KEY);
            observabilityHeader.put("modifiedDate", LocalDateTime.now().toString());
        } else {
            ObservabilityHeader header = createObservabilityHeader(valueObject);
            observabilityObject.put(OBSERVABILITY_HEADER_KEY, new JSONObject(objectMapper.writeValueAsString(header)));
        }

        return enrichedObject.toString();
    }

    /**
     * Injects error metadata descriptors into failed transformation packets.
     */
    public String enrichErrorTopicMessage(String originalValue, Exception exception) throws JsonProcessingException {
        logger.debug("Enriching failed error message diagnostic block");
        JSONObject enrichedObject = new JSONObject(originalValue);
        JSONObject observabilityObject = getOrCreateObservabilityObject(enrichedObject);

        JSONObject observabilityError = observabilityObject.has(OBSERVABILITY_ERROR_KEY)
                ? observabilityObject.getJSONObject(OBSERVABILITY_ERROR_KEY)
                : new JSONObject();

        ErrorDetails errorDetails = determineErrorDetails(exception);

        observabilityError.put("stageCode", STAGE_CODE);
        observabilityError.put("reportingSystemEnvironment", currentEnvironment);
        observabilityError.put("reportingSystemId", REPORTING_SYSTEM_ID);
        observabilityError.put("reportingSystemInstance", "");
        observabilityError.put("reportingSystemType", REPORTING_SYSTEM_TYPE);

        observabilityError.put("errorCode", errorDetails.code);
        observabilityError.put("errorDescription", errorDetails.description);
        observabilityError.put("errorType", errorDetails.type);

        observabilityObject.put(OBSERVABILITY_ERROR_KEY, observabilityError);

        return enrichedObject.toString();
    }

    private JSONObject getOrCreateObservabilityObject(JSONObject root) {
        JSONObject eventObj;
        if (root.has(EVENT_KEY)) {
            eventObj = root.getJSONObject(EVENT_KEY);
        } else {
            eventObj = new JSONObject();
            root.put(EVENT_KEY, eventObj);
        }

        if (eventObj.has(OBSERVABILITY_KEY)) {
            return eventObj.getJSONObject(OBSERVABILITY_KEY);
        } else {
            JSONObject obsObj = new JSONObject();
            eventObj.put(OBSERVABILITY_KEY, obsObj);
            return obsObj;
        }
    }

    private ObservabilityHeader createObservabilityHeader(JSONObject valueObject) {
        ObservabilityHeader header = new ObservabilityHeader();
        header.setDataObjectName("MATERIAL");

        String fallbackId = "TX-" + java.util.UUID.randomUUID().toString().substring(0, 8);
        header.setDocumentId(valueObject.optString("materialNumber", fallbackId));
        header.setTransactionId(fallbackId);
        header.setLocationId("");
        return header;
    }

    private ErrorDetails determineErrorDetails(Exception exception) {
        if (exception instanceof NullPointerException) {
            return new ErrorDetails("RUNTIME_ERROR", "NPE_500", "Encountered missing structural mandatory null values.");
        } else if (exception instanceof IllegalArgumentException) {
            return new ErrorDetails("VALIDATION_ERROR", "VAL_400", exception.getMessage());
        }
        return new ErrorDetails("SYSTEM_ERROR", "SYS_500", exception.getMessage() != null ? exception.getMessage() : "Unknown execution crash.");
    }

    private static class ErrorDetails {
        final String type;
        final String code;
        final String description;

        ErrorDetails(String type, String code, String description) {
            this.type = type;
            this.code = code;
            this.description = description;
        }
    }
}