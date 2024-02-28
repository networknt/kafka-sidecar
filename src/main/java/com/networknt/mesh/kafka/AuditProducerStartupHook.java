package com.networknt.mesh.kafka;

import com.networknt.kafka.producer.NativeLightProducer;
import com.networknt.service.SingletonServiceFactory;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.networknt.server.StartupHookProvider;


public class AuditProducerStartupHook implements StartupHookProvider {

    private static Logger logger = LoggerFactory.getLogger(AuditProducerStartupHook.class);
    public static Producer auditProducer;
    @Override
    public void onStartup() {
        logger.info("AuditProducerStartupHook begins");
        NativeLightProducer lightProducer = SingletonServiceFactory.getBean(NativeLightProducer.class);
        lightProducer.open();
        auditProducer = lightProducer.getProducer();
        logger.info("AuditProducerStartupHook ends");
    }

}
