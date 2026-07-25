package com.venky.demos.kstream.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ObservabilityLine {

    @JsonProperty("documentId")
    private String documentId;

    @JsonProperty("errorCode")
    private String errorCode;

    @JsonProperty("errorDescription")
    private String errorDescription;

    @JsonProperty("errorName")
    private String errorName;

    @JsonProperty("errorSeverity")
    private String errorSeverity;

    @JsonProperty("executionDate")
    private String executionDate;

    @JsonProperty("identifierName1") private String identifierName1;
    @JsonProperty("identifierName2") private String identifierName2;
    @JsonProperty("identifierName3") private String identifierName3;
    @JsonProperty("identifierName4") private String identifierName4;
    @JsonProperty("identifierName5") private String identifierName5;

    @JsonProperty("identifierValue1") private String identifierValue1;
    @JsonProperty("identifierValue2") private String identifierValue2;
    @JsonProperty("identifierValue3") private String identifierValue3;
    @JsonProperty("identifierValue4") private String identifierValue4;
    @JsonProperty("identifierValue5") private String identifierValue5;

    @JsonProperty("reportingSystemEnvironment")
    private String reportingSystemEnvironment;

    @JsonProperty("reportingSystemId")
    private String reportingSystemId;

    @JsonProperty("reportingSystemInstance")
    private String reportingSystemInstance;

    @JsonProperty("reportingSystemType")
    private String reportingSystemType;

    @JsonProperty("stageCode")
    private String stageCode;

    @JsonProperty("status")
    private String status;

    @JsonProperty("transactionId")
    private String transactionId;

    @JsonProperty("errorType")
    private String errorType;

    @JsonProperty("componentName")
    private String componentName;

    public ObservabilityLine() {}

    // Getters and Setters
    public String getDocumentId() { return documentId; }
    public void setDocumentId(String documentId) { this.documentId = documentId; }

    public String getErrorCode() { return errorCode; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }

    public String getErrorDescription() { return errorDescription; }
    public void setErrorDescription(String errorDescription) { this.errorDescription = errorDescription; }

    public String getErrorName() { return errorName; }
    public void setErrorName(String errorName) { this.errorName = errorName; }

    public String getErrorSeverity() { return errorSeverity; }
    public void setErrorSeverity(String errorSeverity) { this.errorSeverity = errorSeverity; }

    public String getExecutionDate() { return executionDate; }
    public void setExecutionDate(String executionDate) { this.executionDate = executionDate; }

    public String getReportingSystemEnvironment() { return reportingSystemEnvironment; }
    public void setReportingSystemEnvironment(String reportingSystemEnvironment) { this.reportingSystemEnvironment = reportingSystemEnvironment; }

    public String getReportingSystemId() { return reportingSystemId; }
    public void setReportingSystemId(String reportingSystemId) { this.reportingSystemId = reportingSystemId; }

    public String getReportingSystemInstance() { return reportingSystemInstance; }
    public void setReportingSystemInstance(String reportingSystemInstance) { this.reportingSystemInstance = reportingSystemInstance; }

    public String getReportingSystemType() { return reportingSystemType; }
    public void setReportingSystemType(String reportingSystemType) { this.reportingSystemType = reportingSystemType; }

    public String getStageCode() { return stageCode; }
    public void setStageCode(String stageCode) { this.stageCode = stageCode; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getErrorType() { return errorType; }
    public void setErrorType(String errorType) { this.errorType = errorType; }

    public String getComponentName() { return componentName; }
    public void setComponentName(String componentName) { this.componentName = componentName; }

    // Dynamic array style mappings can also be bound directly here if extended
}