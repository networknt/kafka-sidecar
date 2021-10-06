package com.networknt.mesh.kafka;

import com.networknt.server.ShutdownHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqldbReactiveConsumerShutdownHook implements ShutdownHookProvider {
    private static Logger logger = LoggerFactory.getLogger(KsqldbReactiveConsumerShutdownHook.class);

    @Override
    public void onShutdown() {
        logger.info("KsqldbReactiveConsumerShutdownHook begins");
        if(KsqldbReactiveConsumerStartupHook.client != null) {
            KsqldbReactiveConsumerStartupHook.client.close();
        }
        logger.info("KsqldbReactiveConsumerShutdownHook ends");
    }

}
