package com.networknt.mesh.kafka;

import com.networknt.config.Config;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.consumer.KafkaConsumerManager;
import com.networknt.server.StartupHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start the passive consumer that listens to the REST calls from the backend Api/App. The Api/App needs to
 * invoke the /consumers/{group} to create a consumer group and start a consumer instance. Then call the
 * /consumers/{group}/instances/{instance}/subscriptions to subscribe one or more topic/partition combination(s).
 * Once the subscriptions is called, the Api/App can call the endpoint /consumers/{group}/instances/{instance}/records
 * to read the Kafka records.
 *
 * @author Steve Hu
 */
public class ActiveConsumerStartupHook implements StartupHookProvider {
    private static Logger logger = LoggerFactory.getLogger(ActiveConsumerStartupHook.class);
    public static KafkaConsumerManager kafkaConsumerManager;
    @Override
    public void onStartup() {
        logger.info("ActiveConsumerStartupHook begins");
        KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
        kafkaConsumerManager = new KafkaConsumerManager(config);
        logger.info("ActiveConsumerStartupHook ends");
    }
}
