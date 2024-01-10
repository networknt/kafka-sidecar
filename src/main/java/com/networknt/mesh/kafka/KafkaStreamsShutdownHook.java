package com.networknt.mesh.kafka;

import com.networknt.server.ShutdownHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamsShutdownHook implements ShutdownHookProvider {
    private static final Logger logger= LoggerFactory.getLogger(KafkaStreamsShutdownHook.class);

    @Override
    public void onShutdown() {
        logger.info("KafkaStreamsShutdownHook Shutdown Begins !!!");

        if(null != KafkaStreamsStartupHook.genericLightStreams){
            KafkaStreamsStartupHook.genericLightStreams.close();
        }
        logger.info("KafkaStreamsShutdownHook Ends !!! ");
    }
}
