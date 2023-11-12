package com.networknt.mesh.kafka;

import com.networknt.config.Config;
import com.networknt.kafka.common.KafkaAdminConfig;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.consumer.LightConsumer;
import com.networknt.server.StartupHookProvider;
import com.networknt.utility.ModuleRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AdminClientStartupHook implements StartupHookProvider {
    private static Logger logger = LoggerFactory.getLogger(AdminClientStartupHook.class);
    public static KafkaAdminConfig config = (KafkaAdminConfig) Config.getInstance().getJsonObjectConfig(KafkaAdminConfig.CONFIG_NAME, KafkaAdminConfig.class);
    public static Admin admin;
    @Override
    public void onStartup() {
        logger.info("AdminClientStartupHook begins");
        admin = Admin.create(config.getProperties());
        // register the module with the configuration properties.
        List<String> masks = new ArrayList<>();
        masks.add("basic.auth.user.info");
        masks.add("sasl.jaas.config");
        masks.add("schema.registry.ssl.truststore.password");
        ModuleRegistry.registerModule(KafkaAdminConfig.CONFIG_NAME, AdminClientStartupHook.class.getName(), Config.getInstance().getJsonMapConfigNoCache(KafkaAdminConfig.CONFIG_NAME), masks);
        logger.info("AdminClientStartupHook ends");
    }
}
