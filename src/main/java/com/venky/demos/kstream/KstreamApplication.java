package com.venky.demos.kstream;

import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KstreamApplication {
    private static final Logger log = LoggerFactory.getLogger(KstreamApplication.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Inside KstreamApplication >>>>>>>");
        SpringApplication.run(KstreamApplication.class, args);
    }

}
