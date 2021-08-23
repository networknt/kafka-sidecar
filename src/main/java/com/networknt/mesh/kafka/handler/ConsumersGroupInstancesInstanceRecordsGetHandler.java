package com.networknt.mesh.kafka.handler;

import com.networknt.config.JsonMapper;
import com.networknt.exception.FrameworkException;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.consumer.*;
import com.networknt.kafka.entity.ConsumerRecord;
import com.networknt.kafka.entity.SidecarConsumerRecord;
import com.networknt.mesh.kafka.ActiveConsumerStartupHook;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class ConsumersGroupInstancesInstanceRecordsGetHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConsumersGroupInstancesInstanceDeleteHandler.class);

    public ConsumersGroupInstancesInstanceRecordsGetHandler () {
        if(logger.isDebugEnabled()) logger.debug("ConsumersGroupInstancesInstanceRecordsGetHandler constructed!");
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        exchange.dispatch();
        String group = exchange.getPathParameters().get("group").getFirst();
        String instance = exchange.getPathParameters().get("instance").getFirst();
        Deque<String> dequeTimeout = exchange.getQueryParameters().get("timeout");
        long timeoutMs = -1;
        if(dequeTimeout != null) {
            timeoutMs = Long.valueOf(dequeTimeout.getFirst());
        }
        Deque<String> dequeMaxBytes = exchange.getQueryParameters().get("maxBytes");
        long maxBytes = -1;
        if(dequeMaxBytes != null) {
            maxBytes = Long.valueOf(dequeMaxBytes.getFirst());
        }
        // String format = exchange.getQueryParameters().get("format").getFirst();
        // TODO find a way to overwrite the default configuration for the keyFormat and valueFormat from the query parameters.
        readRecords(
                exchange,
                group,
                instance,
                Duration.ofMillis(timeoutMs),
                maxBytes,
                KafkaConsumerState.class,
                SidecarConsumerRecord::fromConsumerRecord);
    }

    private <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> void readRecords(
            HttpServerExchange exchange,
            String group,
            String instance,
            Duration timeout,
            long maxBytes,
            Class<KafkaConsumerState>
                    consumerStateType,
            Function<ConsumerRecord<ClientKeyT, ClientValueT>, ?> toJsonWrapper
    ) {
        maxBytes = (maxBytes <= 0) ? Long.MAX_VALUE : maxBytes;
        ActiveConsumerStartupHook.kafkaConsumerManager.readRecords(
                group, instance, consumerStateType, timeout, maxBytes,
                new ConsumerReadCallback<ClientKeyT, ClientValueT>() {
                    @Override
                    public void onCompletion(
                            List<ConsumerRecord<ClientKeyT, ClientValueT>> records, FrameworkException e
                    ) {
                        if (e != null) {
                            if(logger.isDebugEnabled()) logger.debug("FrameworkException:", e);
                            setExchangeStatus(exchange, e.getStatus());
                        } else {
                            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                            exchange.setStatusCode(200);
                            exchange.getResponseSender().send(JsonMapper.toJson(records.stream().map(toJsonWrapper).collect(Collectors.toList())));
                        }
                    }
                }
        );
    }

}
