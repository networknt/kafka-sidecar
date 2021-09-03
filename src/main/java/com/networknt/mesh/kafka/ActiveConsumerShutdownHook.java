package com.networknt.mesh.kafka;

import com.networknt.server.ShutdownHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveConsumerShutdownHook implements ShutdownHookProvider {
    private static Logger logger = LoggerFactory.getLogger(ActiveConsumerShutdownHook.class);

    @Override
    public void onShutdown() {
        logger.info("ActiveConsumerShutdownHook begins");
        if(ActiveConsumerStartupHook.kafkaConsumerManager != null) {
            ActiveConsumerStartupHook.kafkaConsumerManager.shutdown();
        }
        logger.info("ActiveConsumerShutdownHook ends");
    }
}
