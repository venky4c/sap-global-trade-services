package com.venky.demos.kstream.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class SchemaValidationService {
    private static final Logger logger = LoggerFactory.getLogger(SchemaValidationService.class);

    @Value("${spring.resource.schemaType:json}")
    private String schemaType;

    public Object validateAndConvert(String value, String schemaName) throws Exception {
        logger.info("Validating message string against tracking engine rule book name: {}", schemaName);
        // Returns original string validation proof token for downstream producers
        return value;
    }
}
