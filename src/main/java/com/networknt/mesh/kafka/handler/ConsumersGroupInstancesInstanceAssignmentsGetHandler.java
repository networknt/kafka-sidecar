package com.networknt.mesh.kafka.handler;

import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.exception.FrameworkException;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.entity.ConsumerAssignmentRequest;
import com.networknt.kafka.entity.ConsumerAssignmentResponse;
import com.networknt.mesh.kafka.ActiveConsumerStartupHook;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class ConsumersGroupInstancesInstanceAssignmentsGetHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConsumersGroupInstancesInstanceAssignmentsGetHandler.class);

    public ConsumersGroupInstancesInstanceAssignmentsGetHandler () {
        if(logger.isDebugEnabled()) logger.debug("ConsumersGroupInstancesInstanceAssignmentsGetHandler constructed!");
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        String group = exchange.getPathParameters().get("group").getFirst();
        String instance = exchange.getPathParameters().get("instance").getFirst();
        Map<String, Object> map = (Map)exchange.getAttachment(BodyHandler.REQUEST_BODY);
        ConsumerAssignmentRequest request = Config.getInstance().getMapper().convertValue(map, ConsumerAssignmentRequest.class);
        if(logger.isDebugEnabled()) logger.debug("group = {} instance = {} request = {}", group, instance, request);
        try {
            ConsumerAssignmentResponse response = ActiveConsumerStartupHook.kafkaConsumerManager.assignment(group, instance);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
            exchange.setStatusCode(200);
            exchange.getResponseSender().send(JsonMapper.toJson(response));
        } catch (FrameworkException e) {
            if(logger.isDebugEnabled()) logger.debug("FrameworkException", e);
            setExchangeStatus(exchange, e.getStatus());
        }
    }
}
