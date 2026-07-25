package com.venky.demos.kstream.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ObservabilityPayload {

    @JsonProperty("observabilityHeader")
    private ObservabilityHeader observabilityHeader;

    @JsonProperty("observabilityLine")
    private ObservabilityLine observabilityLine;

    @JsonProperty("observabilityError")
    private ObservabilityError observabilityError;

    public ObservabilityPayload() {}

    public ObservabilityHeader getObservabilityHeader() { return observabilityHeader; }
    public void setObservabilityHeader(ObservabilityHeader observabilityHeader) { this.observabilityHeader = observabilityHeader; }

    public ObservabilityLine getObservabilityLine() { return observabilityLine; }
    public void setObservabilityLine(ObservabilityLine observabilityLine) { this.observabilityLine = observabilityLine; }

    public ObservabilityError getObservabilityError() { return observabilityError; }
    public void setObservabilityError(ObservabilityError observabilityError) { this.observabilityError = observabilityError; }

    public String toJson() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failure", e);
        }
    }
}