package com.gridu.exsort;

import java.io.IOException;
import java.util.logging.LogManager;
import java.util.logging.Logger;

class LoggerHandler {
    public static Logger logger;

    static {
        try {
            LogManager.getLogManager().readConfiguration(
                    ExternalSort.class.getResourceAsStream("/log.properties"));
        } catch (IOException e) {
            System.err.println("Could not setup logger configuration: " + e.toString());
        }
        logger = Logger.getLogger(LoggerHandler.class.getName());
    }
}
