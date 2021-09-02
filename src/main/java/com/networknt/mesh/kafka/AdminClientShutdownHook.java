package com.networknt.mesh.kafka;

import com.networknt.server.ShutdownHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminClientShutdownHook  implements ShutdownHookProvider {
    private static Logger logger = LoggerFactory.getLogger(AdminClientShutdownHook.class);

    @Override
    public void onShutdown() {
        logger.info("AdminClientShutdownHook begins");
        if(AdminClientStartupHook.admin != null) {
            AdminClientStartupHook.admin.close();;
        }
        logger.info("AdminClientShutdownHook ends");
    }
}
