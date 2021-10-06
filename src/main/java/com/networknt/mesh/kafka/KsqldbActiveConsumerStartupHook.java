package com.networknt.mesh.kafka;

import com.networknt.config.Config;
import com.networknt.kafka.common.KafkaKsqldbConfig;
import com.networknt.server.StartupHookProvider;
import com.networknt.utility.ModuleRegistry;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class KsqldbActiveConsumerStartupHook implements StartupHookProvider {
    private static Logger logger = LoggerFactory.getLogger(KsqldbActiveConsumerStartupHook.class);
    private static KafkaKsqldbConfig config = (KafkaKsqldbConfig) Config.getInstance().getJsonObjectConfig(KafkaKsqldbConfig.CONFIG_NAME, KafkaKsqldbConfig.class);
    public static Client client = null;

    @Override
    public void onStartup() {
        logger.info("KsqldbActiveConsumerStartupHook begins");
        ClientOptions options;
        if (config.isUseTls()) {
            options = ClientOptions.create()
                    .setHost(config.getKsqldbHost()).setUseTls(true)
                    .setBasicAuthCredentials(config.getBasicAuthCredentialsUser(), config.getBasicAuthCredentialsPassword())
                    .setTrustStore(config.getTrustStore()).setTrustStorePassword(config.getTrustStorePassword()).setUseAlpn(true)
                    .setPort(config.getKsqldbPort());
        } else {
            options = ClientOptions.create()
                    .setHost(config.getKsqldbHost())
                    .setPort(config.getKsqldbPort());
        }
        client = Client.create(options);

        List<String> masks = new ArrayList<>();
        masks.add("basic.auth.user.info");
        masks.add("sasl.jaas.config");
        ModuleRegistry.registerModule(KsqldbActiveConsumerStartupHook.class.getName(), Config.getInstance().getJsonMapConfigNoCache(KafkaKsqldbConfig.CONFIG_NAME), masks);

        logger.debug("KsqldbActiveConsumerStartupHook ends");
    }
}
