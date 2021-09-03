package com.networknt.mesh.kafka;
import com.networknt.kafka.producer.LightProducer;
import com.networknt.server.ShutdownHookProvider;
import com.networknt.service.SingletonServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerShutdownHook implements ShutdownHookProvider {
    private static Logger logger = LoggerFactory.getLogger(ProducerShutdownHook.class);

    @Override
    public void onShutdown() {
        logger.info("ProducerStartupHook begins");
        LightProducer producer = SingletonServiceFactory.getBean(LightProducer.class);
        if(producer != null) {
            producer.close();
        }
        logger.info("ProducerStartupHook ends");
    }
}
