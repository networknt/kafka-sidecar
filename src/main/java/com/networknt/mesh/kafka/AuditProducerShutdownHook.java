package com.networknt.mesh.kafka;

import com.networknt.kafka.producer.LightProducer;
import com.networknt.server.ShutdownHookProvider;
import com.networknt.service.SingletonServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditProducerShutdownHook implements ShutdownHookProvider {

    private static Logger logger = LoggerFactory.getLogger(AuditProducerShutdownHook.class);

    @Override
    public void onShutdown() {
        logger.info("AuditProducerShutdownHook begins");
        LightProducer producer = SingletonServiceFactory.getBean(LightProducer.class);
        if(producer != null) {
            producer.close();
        }
        logger.info("AuditProducerShutdownHook ends");
    }
    
}
