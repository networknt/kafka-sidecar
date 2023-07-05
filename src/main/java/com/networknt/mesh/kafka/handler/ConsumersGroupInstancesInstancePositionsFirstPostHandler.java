package com.networknt.mesh.kafka.handler;

import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.exception.FrameworkException;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.entity.ConsumerSeekToRequest;
import com.networknt.mesh.kafka.ActiveConsumerStartupHook;
import io.undertow.server.HttpServerExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class ConsumersGroupInstancesInstancePositionsFirstPostHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConsumersGroupInstancesInstancePositionsFirstPostHandler.class);

    public ConsumersGroupInstancesInstancePositionsFirstPostHandler () {
        if(logger.isDebugEnabled()) logger.debug("ConsumersGroupInstancesInstancePositionsFirstPostHandler constructed!");
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        String group = exchange.getPathParameters().get("group").getFirst();
        String instance = exchange.getPathParameters().get("instance").getFirst();
        Map<String, Object> map = (Map)exchange.getAttachment(BodyHandler.REQUEST_BODY);
        ConsumerSeekToRequest request = Config.getInstance().getMapper().convertValue(map, ConsumerSeekToRequest.class);
        if(logger.isDebugEnabled()) logger.debug("group = {} instance = {} request = {}", group, instance, request);
        try {
            ActiveConsumerStartupHook.kafkaConsumerManager.seekToBeginning(group, instance, request);
            exchange.setStatusCode(204);
            exchange.endExchange();
        } catch (FrameworkException e) {
            if(logger.isDebugEnabled()) logger.debug("FrameworkException:", e);
            setExchangeStatus(exchange, e.getStatus());
        }
    }
}
