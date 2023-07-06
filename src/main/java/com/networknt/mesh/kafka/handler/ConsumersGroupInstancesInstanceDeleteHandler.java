package com.networknt.mesh.kafka.handler;

import com.networknt.exception.FrameworkException;
import com.networknt.handler.LightHttpHandler;
import com.networknt.mesh.kafka.ActiveConsumerStartupHook;
import io.undertow.server.HttpServerExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class ConsumersGroupInstancesInstanceDeleteHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConsumersGroupInstancesInstanceDeleteHandler.class);

    public ConsumersGroupInstancesInstanceDeleteHandler () {
        if(logger.isInfoEnabled()) logger.debug("ConsumersGroupInstancesInstanceDeleteHandler constructed!");
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        String group = exchange.getPathParameters().get("group").getFirst();
        String instance = exchange.getPathParameters().get("instance").getFirst();
        if(logger.isDebugEnabled()) logger.debug("group = {} instance = {}", group, instance);
        try {
            ActiveConsumerStartupHook.kafkaConsumerManager.deleteConsumer(group, instance);
            exchange.setStatusCode(204);
            exchange.endExchange();
        } catch (FrameworkException e) {
            if(logger.isDebugEnabled()) logger.debug("FrameworkException:", e);
            setExchangeStatus(exchange, e.getStatus());
        }
    }
}
