package com.networknt.mesh.kafka.handler;

import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.common.config.KafkaConsumerConfig;
import com.networknt.kafka.entity.CreateConsumerInstanceRequest;
import com.networknt.kafka.entity.CreateConsumerInstanceResponse;
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
public class ConsumersGroupPostHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConsumersGroupPostHandler.class);
    private final KafkaConsumerConfig config = KafkaConsumerConfig.load();
    private static final String KEY_FORMAT = "keyFormat";
    private static final String VALUE_FORMAT = "valueFormat";

    public ConsumersGroupPostHandler () {
        if(logger.isDebugEnabled()) logger.debug("ConsumersGroupPostHandler constructed!");
    }


    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        String group = exchange.getPathParameters().get("group").getFirst();
        Map<String, Object> map = (Map)exchange.getAttachment(BodyHandler.REQUEST_BODY);
        if (map.get(KEY_FORMAT) == null) {
            map.put(KEY_FORMAT, config.getKeyFormat());
        }
        if (map.get(VALUE_FORMAT) == null) {
            map.put(VALUE_FORMAT, config.getValueFormat());
        }
        CreateConsumerInstanceRequest request = Config.getInstance().getMapper().convertValue(map, CreateConsumerInstanceRequest.class);
        if(logger.isDebugEnabled()) logger.debug("group = {} request = {}", group, request);
        String instanceId = ActiveConsumerStartupHook.kafkaConsumerManager.createConsumer(group, request.toConsumerInstanceConfig());
        String instanceBaseUri = "/consumers/" + group + "/instances/"  + instanceId;
        CreateConsumerInstanceResponse response = new CreateConsumerInstanceResponse(instanceId, instanceBaseUri);
        exchange.setStatusCode(200);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseSender().send(JsonMapper.toJson(response));
    }
}
