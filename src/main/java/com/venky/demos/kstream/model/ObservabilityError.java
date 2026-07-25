package com.venky.demos.kstream.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ObservabilityError {

    @JsonProperty("applicationCode")
    private String applicationCode;

    @JsonProperty("componentName")
    private String componentName;

    @JsonProperty("componentVersionNumber")
    private String componentVersionNumber;

    @JsonProperty("documentId")
    private String documentId;

    @JsonProperty("customEmailBody")
    private String customEmailBody;

    @JsonProperty("customEmailSubject")
    private String customEmailSubject;

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

    @JsonProperty("reportingSystemEnvironment")
    private String reportingSystemEnvironment;

    @JsonProperty("reportingSystemId")
    private String reportingSystemId;

    @JsonProperty("reportingSystemInstance")
    private String reportingSystemInstance;

    @JsonProperty("transactionId")
    private String transactionId;

    @JsonProperty("locationId")
    private String locationId;

    @JsonProperty("errorType")
    private String errorType;

    // Default Constructor
    public ObservabilityError() {}

    // Getters and Setters
    public String getApplicationCode() { return applicationCode; }
    public void setApplicationCode(String applicationCode) { this.applicationCode = applicationCode; }

    public String getComponentName() { return componentName; }
    public void setComponentName(String componentName) { this.componentName = componentName; }

    public String getComponentVersionNumber() { return componentVersionNumber; }
    public void setComponentVersionNumber(String componentVersionNumber) { this.componentVersionNumber = componentVersionNumber; }

    public String getDocumentId() { return documentId; }
    public void setDocumentId(String documentId) { this.documentId = documentId; }

    public String getCustomEmailBody() { return customEmailBody; }
    public void setCustomEmailBody(String customEmailBody) { this.customEmailBody = customEmailBody; }

    public String getCustomEmailSubject() { return customEmailSubject; }
    public void setCustomEmailSubject(String customEmailSubject) { this.customEmailSubject = customEmailSubject; }

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

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getLocationId() { return locationId; }
    public void setLocationId(String locationId) { this.locationId = locationId; }

    public String getErrorType() { return errorType; }
    public void setErrorType(String errorType) { this.errorType = errorType; }
}