package com.networknt.mesh.kafka.handler;

import com.networknt.config.JsonMapper;
import com.networknt.exception.FrameworkException;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.entity.ConsumerSubscriptionResponse;
import com.networknt.mesh.kafka.ActiveConsumerStartupHook;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class ConsumersGroupInstancesInstanceSubscriptionsGetHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConsumersGroupInstancesInstanceSubscriptionsGetHandler.class);

    public ConsumersGroupInstancesInstanceSubscriptionsGetHandler () {
        if(logger.isDebugEnabled()) logger.debug("ConsumersGroupInstancesInstanceSubscriptionsGetHandler constructed!");
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        String group = exchange.getPathParameters().get("group").getFirst();
        String instance = exchange.getPathParameters().get("instance").getFirst();
        if(logger.isDebugEnabled()) logger.debug("group = {} instance = {}", group, instance);
        try {
            ConsumerSubscriptionResponse response = ActiveConsumerStartupHook.kafkaConsumerManager.subscription(group, instance);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
            exchange.setStatusCode(200);
            exchange.getResponseSender().send(JsonMapper.toJson(response));
        } catch (FrameworkException e) {
            if(logger.isDebugEnabled()) logger.debug("FrameworkException", e);
            setExchangeStatus(exchange, e.getStatus());
        }
    }
}
