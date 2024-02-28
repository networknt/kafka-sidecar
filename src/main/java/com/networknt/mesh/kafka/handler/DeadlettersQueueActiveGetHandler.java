package com.networknt.mesh.kafka.handler;

import com.networknt.client.Http2Client;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.exception.FrameworkException;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.consumer.ConsumerReadCallback;
import com.networknt.kafka.consumer.KafkaConsumerState;
import com.networknt.kafka.entity.*;
import com.networknt.mesh.kafka.ActiveConsumerStartupHook;

import io.undertow.client.ClientConnection;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class DeadlettersQueueActiveGetHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(DeadlettersQueueActiveGetHandler.class);
    public static KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    long maxBytes = -1;
    public static ClientConnection connection;
    public static Http2Client client = Http2Client.getInstance();
    private static String REPLAY_DEFAULT_INSTANCE = "Active-Replay-1289990";

    public DeadlettersQueueActiveGetHandler() {
        if(logger.isDebugEnabled()) logger.debug("ReplayDeadLetterTopicGetHandler constructed!");
    }


    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        String groupId = exchange.getQueryParameters().get("group")==null? config.getGroupId() : exchange.getQueryParameters().get("group").getFirst();
        KafkaConsumerState state = ActiveConsumerStartupHook.kafkaConsumerManager.getExistingConsumerInstance(groupId, REPLAY_DEFAULT_INSTANCE);
        String instanceId = REPLAY_DEFAULT_INSTANCE;
        if (state == null) {
             CreateConsumerInstanceRequest request = new CreateConsumerInstanceRequest(REPLAY_DEFAULT_INSTANCE, null, EmbeddedFormat.STRING.name(), EmbeddedFormat.STRING.name(), null, null, null, null);
            instanceId = ActiveConsumerStartupHook.kafkaConsumerManager.createConsumer(groupId, request.toConsumerInstanceConfig());
        }

        Deque<String> dequeTimeout = exchange.getQueryParameters().get("timeout");
        long timeoutMs = -1;
        if(dequeTimeout != null) {
            timeoutMs = Long.valueOf(dequeTimeout.getFirst());
        }
        String topic = exchange.getQueryParameters().get("topic")==null? config.getTopic() : exchange.getQueryParameters().get("topic").getFirst();
        ConsumerSubscriptionRecord subscription;
        if(topic.contains(",")) {
            // remove the whitespaces
            topic = topic.replaceAll("\\s+","");
            subscription = new ConsumerSubscriptionRecord(Arrays.asList(topic.split(",", -1)).stream().map(t->t + config.getDeadLetterTopicExt()).collect(Collectors.toList()), null);
        } else {
            subscription = new ConsumerSubscriptionRecord(Collections.singletonList(topic + config.getDeadLetterTopicExt()), null);
        }
        ActiveConsumerStartupHook.kafkaConsumerManager.subscribe(groupId, instanceId, subscription);

        exchange.dispatch();
        readRecords(
                exchange,
                groupId,
                instanceId,
                Duration.ofMillis(timeoutMs),
                subscription.getTopics(),
                KafkaConsumerState.class,
                SidecarConsumerRecord::fromConsumerRecord);
    }

    private <ClientKeyT, ClientValueT> void readRecords(
            HttpServerExchange exchange,
            String group,
            String instance,
            Duration timeout,
            List<String> topics,
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
                            if(logger.isDebugEnabled()) logger.debug("polled records size = " + records.size());
                            if(logger.isDebugEnabled()) logger.debug("total dlq records processed:" + records.size());
                            ActiveConsumerStartupHook.kafkaConsumerManager.commitCurrentOffsets(group, instance);
                            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                            exchange.setStatusCode(200);
                            exchange.getResponseSender().send(JsonMapper.toJson(records.stream().map(toJsonWrapper).collect(Collectors.toList())));

                        }
                    }
                }
        );

    }

}
