package com.networknt.mesh.kafka.handler;

import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.exception.FrameworkException;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.consumer.KafkaConsumerManager;
import com.networknt.kafka.entity.CommitOffsetsResponse;
import com.networknt.kafka.entity.ConsumerOffsetCommitRequest;
import com.networknt.kafka.entity.TopicPartitionOffset;
import com.networknt.mesh.kafka.ActiveConsumerStartupHook;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class ConsumersGroupInstancesInstanceOffsetsPostHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConsumersGroupInstancesInstanceOffsetsPostHandler.class);

    public ConsumersGroupInstancesInstanceOffsetsPostHandler () {
        if(logger.isDebugEnabled()) logger.debug("ConsumersGroupInstancesInstanceOffsetsPostHandler constructed!");
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        String group = exchange.getPathParameters().get("group").getFirst();
        String instance = exchange.getPathParameters().get("instance").getFirst();
        Deque<String> dequeAsync = exchange.getQueryParameters().get("async");
        boolean async = false;
        if(dequeAsync != null) {
            async = Boolean.valueOf(dequeAsync.getFirst());
        }
        Map<String, Object> map = (Map)exchange.getAttachment(BodyHandler.REQUEST_BODY);
        ConsumerOffsetCommitRequest request = Config.getInstance().getMapper().convertValue(map, ConsumerOffsetCommitRequest.class);
        if(logger.isDebugEnabled()) logger.debug("group = {} instance = {} async = {} request = {}", group, instance, async, request);
        exchange.dispatch();
        ActiveConsumerStartupHook.kafkaConsumerManager.commitOffsets(
                group,
                instance,
                async,
                request,
                new KafkaConsumerManager.CommitCallback() {
                    @Override
                    public void onCompletion(
                            List<TopicPartitionOffset> offsets,
                            FrameworkException e
                    ) {
                        if (e != null) {
                            setExchangeStatus(exchange, e.getStatus());
                        } else {
                            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                            exchange.setStatusCode(200);
                            exchange.getResponseSender().send(JsonMapper.toJson(CommitOffsetsResponse.fromOffsets(offsets)));
                        }
                    }
                }
        );
    }
}
