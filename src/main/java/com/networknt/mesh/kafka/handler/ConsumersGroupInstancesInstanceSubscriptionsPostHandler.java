package com.networknt.mesh.kafka.handler;

import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.exception.FrameworkException;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.entity.ConsumerSubscriptionRecord;
import com.networknt.mesh.kafka.ActiveConsumerStartupHook;
import io.undertow.server.HttpServerExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class ConsumersGroupInstancesInstanceSubscriptionsPostHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConsumersGroupInstancesInstanceSubscriptionsPostHandler.class);

    public ConsumersGroupInstancesInstanceSubscriptionsPostHandler () {
        if(logger.isDebugEnabled()) logger.debug("ConsumersGroupInstancesInstanceSubscriptionsPostHandler constructed!");
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        String group = exchange.getPathParameters().get("group").getFirst();
        String instance = exchange.getPathParameters().get("instance").getFirst();
        Map<String, Object> map = (Map)exchange.getAttachment(BodyHandler.REQUEST_BODY);
        ConsumerSubscriptionRecord subscription = Config.getInstance().getMapper().convertValue(map, ConsumerSubscriptionRecord.class);
        if(logger.isDebugEnabled()) logger.debug("group = {} instance = {} subscription = {}", group, instance, subscription);
        try {
            ActiveConsumerStartupHook.kafkaConsumerManager.subscribe(group, instance, subscription);
            exchange.setStatusCode(204);
            exchange.endExchange();
        } catch (FrameworkException e) {
            if(logger.isDebugEnabled()) logger.debug("FrameworkException:", e);
            setExchangeStatus(exchange, e.getStatus());
        }
    }
}
