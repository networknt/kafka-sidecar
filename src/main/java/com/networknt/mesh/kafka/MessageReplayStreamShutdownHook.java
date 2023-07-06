package com.networknt.mesh.kafka;

import com.networknt.server.ShutdownHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageReplayStreamShutdownHook implements ShutdownHookProvider {

    private static final Logger logger= LoggerFactory.getLogger(MessageReplayStreamShutdownHook.class);

    @Override
    public void onShutdown() {

        logger.info("MessageReplayStreamShutdownHook Shutdown Begins !!!");

        if(null != MessageReplayStreamStartupHook.messageReplayStreams){
            MessageReplayStreamStartupHook.messageReplayStreams.close();
        }

        logger.info("MessageReplayStreamShutdownHook Ends !!!");

    }
}
