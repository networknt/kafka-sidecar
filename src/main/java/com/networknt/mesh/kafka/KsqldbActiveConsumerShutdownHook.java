package com.networknt.mesh.kafka;

import com.networknt.server.ShutdownHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqldbActiveConsumerShutdownHook implements ShutdownHookProvider {
    private static Logger logger = LoggerFactory.getLogger(KsqldbActiveConsumerShutdownHook.class);

    @Override
    public void onShutdown() {
        logger.info("KsqldbActiveConsumerShutdownHook begins");
        if(KsqldbActiveConsumerStartupHook.client != null) {
            KsqldbActiveConsumerStartupHook.client.close();
        }
        logger.info("KsqldbActiveConsumerShutdownHook ends");
    }

}
