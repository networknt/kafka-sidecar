package com.networknt.mesh.kafka;

import com.networknt.server.ShutdownHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqldbConsumerShutdownHook implements ShutdownHookProvider {
    private static Logger logger = LoggerFactory.getLogger(KsqldbConsumerShutdownHook.class);

    @Override
    public void onShutdown() {
        logger.debug("KsqldbConsumerShutdownHook begins");
        if(KsqldbConsumerStartupHook.client != null) {
            KsqldbConsumerStartupHook.client.close();
        }
        logger.debug("KsqldbConsumerShutdownHook ends");
    }

}
