package com.networknt.mesh.kafka;

import com.networknt.config.Config;
import com.networknt.kafka.common.KafkaAdminConfig;
import com.networknt.server.StartupHookProvider;
import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminClientStartupHook implements StartupHookProvider {
    private static Logger logger = LoggerFactory.getLogger(AdminClientStartupHook.class);
    public static KafkaAdminConfig config = (KafkaAdminConfig) Config.getInstance().getJsonObjectConfig(KafkaAdminConfig.CONFIG_NAME, KafkaAdminConfig.class);
    public static Admin admin;
    @Override
    public void onStartup() {
        logger.debug("AdminClientStartupHook begins");
        admin = Admin.create(config.getProperties());
        logger.debug("AdminClientStartupHook ends");
    }

}
