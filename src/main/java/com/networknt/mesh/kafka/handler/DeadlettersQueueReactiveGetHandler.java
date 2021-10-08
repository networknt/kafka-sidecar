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
import com.networknt.mesh.kafka.ProducerStartupHook;
import com.networknt.mesh.kafka.ReactiveConsumerStartupHook;
import com.networknt.mesh.kafka.SidecarAuditHelper;
import com.networknt.mesh.kafka.WriteAuditLog;
import com.networknt.server.Server;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.OptionMap;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class DeadlettersQueueReactiveGetHandler extends WriteAuditLog implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(DeadlettersQueueReactiveGetHandler.class);
    public static KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    long maxBytes = -1;
    public static ClientConnection connection;
    public static Http2Client client = Http2Client.getInstance();
    private static String UNEXPECTED_CONSUMER_READ_EXCEPTION = "ERR12205";
    private static String INVALID_TOPIC_NAME = "ERR30001";
    private static String REPLAY_DEFAULT_INSTANCE = "Reactive-Replay-1289990";
    private  boolean lastRetry = false;

    public DeadlettersQueueReactiveGetHandler() {
        if(logger.isDebugEnabled()) logger.debug("ReplayDeadLetterTopicGetHandler constructed!");
    }

    
    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        String groupId = exchange.getQueryParameters().get("group")==null? config.getGroupId() : exchange.getQueryParameters().get("group").getFirst();
        KafkaConsumerState state = ReactiveConsumerStartupHook.kafkaConsumerManager.getExistingConsumerInstance(groupId, REPLAY_DEFAULT_INSTANCE);
        String instanceId = REPLAY_DEFAULT_INSTANCE;
        if (state == null) {
             CreateConsumerInstanceRequest request = new CreateConsumerInstanceRequest(REPLAY_DEFAULT_INSTANCE, null, EmbeddedFormat.STRING.name(), EmbeddedFormat.STRING.name(), null, null, null, null);
            instanceId = ReactiveConsumerStartupHook.kafkaConsumerManager.createConsumer(groupId, request.toConsumerInstanceConfig());
        }

        if (exchange.getQueryParameters().get("lastretry")!=null) {
            lastRetry = Boolean.parseBoolean(exchange.getQueryParameters().get("lastretry").getFirst());
        }

        Deque<String> dequeTimeout = exchange.getQueryParameters().get("timeout");
        long timeoutMs = -1;
        if(dequeTimeout != null) {
            timeoutMs = Long.valueOf(dequeTimeout.getFirst());
        }
        String topic;
        String configTopic = config.getTopic();
        List<String> configTopics;
        if(configTopic.contains(",")) {
            configTopic = configTopic.replaceAll("\\s+","");
            configTopics = Arrays.asList(configTopic.split(",", -1));
        } else {
            configTopics = Collections.singletonList(configTopic);
        }
        List<String> topics;
        if (exchange.getQueryParameters().get("topic")==null) {
            topics = configTopics;
        } else {
            topic = exchange.getQueryParameters().get("topic").getFirst();
            if(topic.contains(",")) {
                topic = topic.replaceAll("\\s+","");
                topics = Arrays.asList(topic.split(",", -1));
            } else {
                topics = Collections.singletonList(topic);
            }
            if (!configTopics.containsAll(topics)) {
                setExchangeStatus(exchange, INVALID_TOPIC_NAME);
                return;
            }
        }
     //   String topic = exchange.getQuer(Arrays.asListyParameters().get("topic")==null? config.getTopic() : exchange.getQueryParameters().get("topic").getFirst();
        ConsumerSubscriptionRecord subscription;
        subscription = new ConsumerSubscriptionRecord(topics.stream().map(t->t + config.getDeadLetterTopicExt()).collect(Collectors.toList()), null);
        ReactiveConsumerStartupHook.kafkaConsumerManager.subscribe(groupId, instanceId, subscription);

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
        ReactiveConsumerStartupHook.kafkaConsumerManager.readRecords(
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
                            if(records.size() > 0) {
                                if(logger.isDebugEnabled()) logger.debug("polled records size = " + records.size());
                                if(connection == null || !connection.isOpen()) {
                                    try {
                                        if(config.getBackendApiHost().startsWith("https")) {
                                            connection = client.borrowConnection(new URI(config.getBackendApiHost()), Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true)).get();
                                        } else {
                                            connection = client.borrowConnection(new URI(config.getBackendApiHost()), Http2Client.WORKER, Http2Client.BUFFER_POOL, OptionMap.EMPTY).get();
                                        }
                                    } catch (Exception ex) {
                                        logger.error("Rollback due to connection error to the backend: ", ex);
                                        rollback(records.get(0), group, instance);
                                        setExchangeStatus(exchange, UNEXPECTED_CONSUMER_READ_EXCEPTION);
                                        return;
                                    }
                                }
                                final CountDownLatch latch = new CountDownLatch(1);
                                final AtomicReference<ClientResponse> reference = new AtomicReference<>();
                                try {
                                    ClientRequest request = new ClientRequest().setMethod(Methods.POST).setPath(config.getBackendApiPath());
                                    request.getRequestHeaders().put(Headers.CONTENT_TYPE, "application/json");
                                    request.getRequestHeaders().put(Headers.TRANSFER_ENCODING, "chunked");
                                    request.getRequestHeaders().put(Headers.HOST, "localhost");
                                    if(logger.isInfoEnabled()) logger.info("Send a batch to the backend API");

                                    connection.sendRequest(request, client.createClientCallback(reference, latch, JsonMapper.toJson(records.stream().map(toJsonWrapper).collect(Collectors.toList()))));
                                    latch.await();
                                    int statusCode = reference.get().getResponseCode();
                                    String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
                                    if(logger.isDebugEnabled()) logger.debug("statusCode = " + statusCode + " body  = " + body);
                                    if(statusCode >= 400) {
                                        // something happens on the backend and the data is not consumed correctly.
                                        logger.error("Rollback due to error response from backend with status code = " + statusCode + " body = " + body);
                                        rollback(records.get(0), group, instance);
                                        setExchangeStatus(exchange, UNEXPECTED_CONSUMER_READ_EXCEPTION);
                                    } else {
                                        // The body will contains RecordProcessedResult for dead letter queue and audit.
                                        // Write the dead letter queue if necessary.
                                        if(logger.isInfoEnabled()) logger.info("Got successful response from the backend API");
                                        processResponse(body);
                                        // commit the batch offset here.
                                        ReactiveConsumerStartupHook.kafkaConsumerManager.commitCurrentOffsets(group, instance);
                                        if(logger.isDebugEnabled()) logger.debug("total dlq records processed:" + records.size());
                                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                                        exchange.setStatusCode(200);
                                        DeadLetterQueueReplayResponse deadLetterQueueReplayResponse = new DeadLetterQueueReplayResponse();
                                        deadLetterQueueReplayResponse.setGroup(group);
                                        deadLetterQueueReplayResponse.setTopics(topics);
                                        deadLetterQueueReplayResponse.setInstance(instance);
                                        deadLetterQueueReplayResponse.setRecords(Long.valueOf(records.size()));
                                        ConsumerRecord<ClientKeyT, ClientValueT> record = records.get(records.size()-1);
                                        deadLetterQueueReplayResponse.setDescription("Dead letter queue process successful to partition:" + record.getPartition() +  "| offset:" + record.getOffset());
                                        exchange.getResponseSender().send(JsonMapper.toJson(deadLetterQueueReplayResponse));
                                    }
                                } catch (Exception exception) {
                                    logger.error("Rollback due to process response exception: ", exception);
                                    rollback(records.get(0), group, instance);
                                    setExchangeStatus(exchange, UNEXPECTED_CONSUMER_READ_EXCEPTION);
                                }
                            } else {
                                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                                exchange.setStatusCode(200);
                                DeadLetterQueueReplayResponse deadLetterQueueReplayResponse = new DeadLetterQueueReplayResponse();
                                deadLetterQueueReplayResponse.setGroup(group);
                                deadLetterQueueReplayResponse.setTopics(topics);
                                deadLetterQueueReplayResponse.setInstance(instance);
                                deadLetterQueueReplayResponse.setRecords(0L);
                                deadLetterQueueReplayResponse.setDescription("Dead letter queue process successful to end, no records processed" );
                                exchange.getResponseSender().send(JsonMapper.toJson(deadLetterQueueReplayResponse));
                            }
                        }
                    }
                }
        );

    }

    private void rollback(ConsumerRecord firstRecord, String groupId, String instanceId) {
        ConsumerSeekRequest.PartitionOffset partitionOffset = new ConsumerSeekRequest.PartitionOffset(firstRecord.getTopic(), firstRecord.getPartition(), firstRecord.getOffset(), null);
        List<ConsumerSeekRequest.PartitionOffset> offsets = new ArrayList<>();
        offsets.add(partitionOffset);
        List<ConsumerSeekRequest.PartitionTimestamp> timestamps = new ArrayList<>();
        ConsumerSeekRequest consumerSeekRequest = new ConsumerSeekRequest(offsets, timestamps);
        ReactiveConsumerStartupHook.kafkaConsumerManager.seek(groupId, instanceId, consumerSeekRequest);
        if(logger.isDebugEnabled()) logger.debug("Rollback to topic " + firstRecord.getTopic() + " partition " + firstRecord.getPartition() + " offset " + firstRecord.getOffset());
    }

    private void processResponse(String responseBody) {
        if(responseBody != null) {
            List<Map<String, Object>> results = JsonMapper.string2List(responseBody);
            for(int i = 0; i < results.size(); i ++) {
                RecordProcessedResult result = Config.getInstance().getMapper().convertValue(results.get(i), RecordProcessedResult.class);
                if(config.isDeadLetterEnabled() && !result.isProcessed() && !lastRetry) {
                    ProducerStartupHook.producer.send(
                            new ProducerRecord<>(
                                    result.getRecord().getTopic() + config.getDeadLetterTopicExt(),
                                    null,
                                    System.currentTimeMillis(),
                                    null,
                                    JsonMapper.toJson(result).getBytes(StandardCharsets.UTF_8),
                                    null),
                            (metadata, exception) -> {
                                if (exception != null) {
                                    // handle the exception by logging an error;
                                    logger.error("Exception:" + exception);
                                } else {
                                    if(logger.isTraceEnabled()) logger.trace("Write back to dead letter topic meta " + metadata.topic() + " " + metadata.partition() + " " + metadata.offset());
                                }
                            });
                }
                if(config.isAuditEnabled())  reactiveConsumerAuditLog(result, config.getAuditTarget(), config.getAuditTopic());
            }
        }

    }

}
