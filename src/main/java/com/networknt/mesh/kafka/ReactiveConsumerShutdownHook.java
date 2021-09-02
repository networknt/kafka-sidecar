package com.networknt.mesh.kafka;

import com.networknt.server.ShutdownHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReactiveConsumerShutdownHook implements ShutdownHookProvider {
    private static Logger logger = LoggerFactory.getLogger(ReactiveConsumerShutdownHook.class);

    @Override
    public void onShutdown() {
        logger.info("ReactiveConsumerShutdownHook begins");
        if(ReactiveConsumerStartupHook.kafkaConsumerManager != null) {
            ReactiveConsumerStartupHook.kafkaConsumerManager.shutdown();
        }
        logger.info("ReactiveConsumerShutdownHook ends");
    }

}
