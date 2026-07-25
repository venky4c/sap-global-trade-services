package com.venky.demos.kstream.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class ObservabilityHeader {

    @JsonProperty("dataObjectName")
    private String dataObjectName;

    @JsonProperty("documentId")
    private String documentId;

    @JsonProperty("locationId")
    private String locationId;

    @JsonProperty("transactionId")
    private String transactionId;

    @JsonProperty("modifiedDate")
    private String modifiedDate = LocalDateTime.now(ZoneOffset.UTC)
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));

    // Default Constructor
    public ObservabilityHeader() {}

    // Getters and Setters
    public String getDataObjectName() { return dataObjectName; }
    public void setDataObjectName(String dataObjectName) { this.dataObjectName = dataObjectName; }

    public String getDocumentId() { return documentId; }
    public void setDocumentId(String documentId) { this.documentId = documentId; }

    public String getLocationId() { return locationId; }
    public void setLocationId(String locationId) { this.locationId = locationId; }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getModifiedDate() { return modifiedDate; }
    public void setModifiedDate(String modifiedDate) { this.modifiedDate = modifiedDate; }

    @Override
    public String toString() {
        return "ObservabilityHeader{" +
                "dataObjectName='" + dataObjectName + '\'' +
                ", documentId='" + documentId + '\'' +
                ", locationId='" + locationId + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", modifiedDate='" + modifiedDate + '\'' +
                '}';
    }
}