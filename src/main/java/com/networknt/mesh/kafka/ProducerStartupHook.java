package com.networknt.mesh.kafka;

import com.networknt.kafka.producer.NativeLightProducer;
import com.networknt.kafka.producer.RegisteredSchema;
import com.networknt.service.SingletonServiceFactory;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.networknt.server.StartupHookProvider;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ProducerStartupHook implements StartupHookProvider {
    private static Logger logger = LoggerFactory.getLogger(ProducerStartupHook.class);
    public static Producer producer;
    @Override
    public void onStartup() {
        logger.info("ProducerStartupHook begins");
        NativeLightProducer lightProducer = SingletonServiceFactory.getBean(NativeLightProducer.class);
        lightProducer.open();
        producer = lightProducer.getProducer();
        logger.info("ProducerStartupHook ends");
    }
}
