package com.networknt.mesh.kafka;

import com.networknt.kafka.producer.NativeLightProducer;
import com.networknt.service.SingletonServiceFactory;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.networknt.server.StartupHookProvider;

public class ProducerStartupHook implements StartupHookProvider {
    private static Logger logger = LoggerFactory.getLogger(ProducerStartupHook.class);
    public static Producer producer;
    @Override
    public void onStartup() {
        logger.debug("ProducerStartupHook begins");
        NativeLightProducer lightProducer = SingletonServiceFactory.getBean(NativeLightProducer.class);
        lightProducer.open();
        producer = lightProducer.getProducer();
        logger.debug("ProducerStartupHook ends");
    }
}
