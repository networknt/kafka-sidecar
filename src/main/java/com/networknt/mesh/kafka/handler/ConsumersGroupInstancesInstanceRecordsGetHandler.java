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
        Deque<String> dequeMaxBytes = exchange.getQueryParameters().get("maxBytes");

        try{
            String result = activeReadRecordUtil(group,instance,dequeTimeout, dequeMaxBytes);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
            exchange.setStatusCode(200);
            exchange.getResponseSender().send(result);
        }
        catch(FrameworkException e){
            if(logger.isDebugEnabled()) logger.debug("FrameworkException: ", e);
            setExchangeStatus(exchange, e.getStatus());
        }
    }


    public String activeReadRecordUtil(String group,String instance, Deque<String> dequeTimeout ,Deque<String> dequeMaxBytes){

        long timeoutMs = -1;
        if(dequeTimeout != null) {
            timeoutMs = Long.valueOf(dequeTimeout.getFirst());
        }

        long maxBytes = -1;
        if(dequeMaxBytes != null) {
            maxBytes = Long.valueOf(dequeMaxBytes.getFirst());
        }
        // String format = exchange.getQueryParameters().get("format").getFirst();
        // TODO find a way to overwrite the default configuration for the keyFormat and valueFormat from the query parameters.
        try {
            return readRecords(
                    group,
                    instance,
                    Duration.ofMillis(timeoutMs),
                    maxBytes,
                    KafkaConsumerState.class,
                    SidecarConsumerRecord::fromConsumerRecord);
        }
        catch(FrameworkException e){
            throw new FrameworkException(e.getStatus());
        }

    }

    private <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> String readRecords(
            String group,
            String instance,
            Duration timeout,
            long maxBytes,
            Class<KafkaConsumerState>
                    consumerStateType,
            Function<ConsumerRecord<ClientKeyT, ClientValueT>, ?> toJsonWrapper
    ) {
        maxBytes = (maxBytes <= 0) ? Long.MAX_VALUE : maxBytes;
        final String[] result = {null};
        ActiveConsumerStartupHook.kafkaConsumerManager.readRecords(
                group, instance, consumerStateType, timeout, maxBytes,
                new ConsumerReadCallback<ClientKeyT, ClientValueT>() {
                    @Override
                    public void onCompletion(
                            List<ConsumerRecord<ClientKeyT, ClientValueT>> records, FrameworkException e
                    ) {
                        if (e != null) {
                            throw new FrameworkException( e.getStatus());
                        } else {
                            result[0] = JsonMapper.toJson(records.stream().map(toJsonWrapper).collect(Collectors.toList()));
                        }
                    }
                }
        );

        return result[0];
    }

}
