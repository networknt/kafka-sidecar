package com.networknt.mesh.kafka.util;

import com.networknt.kafka.common.config.KafkaConsumerConfig;
import com.networknt.kafka.consumer.KafkaConsumerManager;

public class KafkaConsumerManagerFactory {

    private KafkaConsumerManagerFactory(){}

    public static KafkaConsumerManager createKafkaConsumerManager(KafkaConsumerConfig config) {
        return new KafkaConsumerManager(config);
    }
}
