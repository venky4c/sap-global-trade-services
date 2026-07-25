package com.venky.demos.kstream.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

@Service
public class LocalSchemaValidationService {
    private static final Logger logger = LoggerFactory.getLogger(LocalSchemaValidationService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private JsonSchema localJsonSchema;

    public Object validateAndConvert(String value, String schemaName) throws Exception {
        logger.info("Validating message against local schema for: {}", schemaName);
        if (localJsonSchema == null) {
            loadLocalSchema();
        }

        // Parse incoming raw JSON string text
        JsonNode jsonNode = objectMapper.readTree(value);

        // Execute the validation schema rules
        Set<ValidationMessage> errors = localJsonSchema.validate(jsonNode);

        if (!errors.isEmpty()) {
            StringBuilder sb = new StringBuilder("Schema validation failed: ");
            for (ValidationMessage msg : errors) {
                sb.append(msg.getMessage()).append("; ");
            }
            throw new RuntimeException(sb.toString());
        }

        logger.info("Successfully validated payload structure locally.");
        return value;
    }

    private void loadLocalSchema() throws IOException {
        try (InputStream is = new ClassPathResource("schema/schema.json").getInputStream()) {
            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
            localJsonSchema = factory.getSchema(is);
            logger.info("Successfully loaded local JSON schema from schema/schema.json");
        } catch (Exception e) {
            logger.error("Failed to load local JSON schema file: {}", e.getMessage(), e);
            throw new IOException("Could not load local schema file", e);
        }
    }

    public void reloadSchema() throws IOException {
        localJsonSchema = null;
        loadLocalSchema();
    }

    public boolean isSchemaLoaded() {
        return localJsonSchema != null;
    }
}