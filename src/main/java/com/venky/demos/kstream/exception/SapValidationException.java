package com.venky.demos.kstream.exception;

public class SapValidationException extends RuntimeException {

    private final String errorCode;
    private final String failedFieldName;

    public SapValidationException(String message, String errorCode, String failedFieldName) {
        super(message);
        this.errorCode = errorCode;
        this.failedFieldName = failedFieldName;
    }

    public String getErrorCode() { return errorCode; }
    public String getFailedFieldName() { return failedFieldName; }
}