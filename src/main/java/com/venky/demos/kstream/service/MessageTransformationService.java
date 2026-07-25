package com.venky.demos.kstream.service;


import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MessageTransformationService {
    private static final Logger logger = LoggerFactory.getLogger(MessageTransformationService.class);

    public JSONObject transformMessage(JSONObject message) {
        logger.info("Received message payload in transformation service wrapper");
        // Deep copy incoming message payload structure
        JSONObject transformed = new JSONObject(message.toString());

        // This is a minimal pass-through stub matching Page 14 requirements
        logger.info("Message transformation completed successfully");
        return transformed;
    }
}
