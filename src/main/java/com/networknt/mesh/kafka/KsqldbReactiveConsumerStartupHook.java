package com.networknt.mesh.kafka;

import com.networknt.config.Config;
import com.networknt.kafka.common.KafkaKsqldbConfig;
import com.networknt.server.StartupHookProvider;
import io.confluent.ksql.api.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KsqldbReactiveConsumerStartupHook implements StartupHookProvider {
    private static Logger logger = LoggerFactory.getLogger(KsqldbReactiveConsumerStartupHook.class);
    private static KafkaKsqldbConfig config = (KafkaKsqldbConfig) Config.getInstance().getJsonObjectConfig(KafkaKsqldbConfig.CONFIG_NAME, KafkaKsqldbConfig.class);
    public static Client client = null;

    @Override
    public void onStartup() {
        logger.info("KsqldbReactiveConsumerStartupHook begins");
        ClientOptions options = ClientOptions.create()
                .setHost(config.getKsqldbHost())
                .setPort(config.getKsqldbPort());
        client = Client.create(options);
        Map<String, Object> properties = config.getProperties();
        try {
            client.streamQuery(config.getQuery(), properties)
                    .thenAccept(streamedQueryResult -> {
                        System.out.println("Query has started. Query ID: " + streamedQueryResult.queryID());
                        RowSubscriber subscriber = new RowSubscriber();
                        streamedQueryResult.subscribe(subscriber);
                    }).exceptionally(e -> {
                System.out.println("Request failed: " + e);
                return null;
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("KsqldbReactiveConsumerStartupHook ends");
    }
}
