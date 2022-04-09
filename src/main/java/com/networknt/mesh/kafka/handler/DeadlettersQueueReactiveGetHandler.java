package com.networknt.mesh.kafka.handler;

import com.networknt.kafka.producer.NativeLightProducer;
import com.networknt.kafka.producer.SidecarProducer;
import com.networknt.mesh.kafka.ProducerStartupHook;
import com.networknt.mesh.kafka.ReactiveConsumerStartupHook;
import com.networknt.mesh.kafka.WriteAuditLog;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.client.Http2Client;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.exception.FrameworkException;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.consumer.ConsumerReadCallback;
import com.networknt.kafka.consumer.KafkaConsumerState;
import com.networknt.kafka.entity.*;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.server.Server;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.status.Status;
import com.networknt.utility.ObjectUtils;
import com.networknt.utility.StringUtils;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.OptionMap;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * For more information on how to write business handlers, please check the link below.
 * https://doc.networknt.com/development/business-handler/rest/
 */
public class DeadlettersQueueReactiveGetHandler extends WriteAuditLog implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(DeadlettersQueueReactiveGetHandler.class);
    public static KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    long maxBytes = -1;
    private static String UNEXPECTED_CONSUMER_READ_EXCEPTION = "ERR12205";
    private static String INVALID_TOPIC_NAME = "ERR30001";
    private static String REPLAY_DEFAULT_INSTANCE = "Reactive-Replay-1289990";
    private  boolean lastRetry = false;
    String instanceId;
    String groupId;
    private AtomicReference<Result<List<ConsumerRecord<Object, Object>>>> result = new AtomicReference<>();
    public List<AuditRecord> auditRecords = new ArrayList<>();
    SidecarProducer lightProducer;

    public DeadlettersQueueReactiveGetHandler() {
        if(ProducerStartupHook.producer != null) {
            lightProducer = (SidecarProducer) SingletonServiceFactory.getBean(NativeLightProducer.class);
        } else {
            logger.error("ProducerStartupHook is not configured in the service.yml and it is needed");
            throw new RuntimeException("ProducerStartupHook is not loaded!");
        }
        if(logger.isDebugEnabled()) logger.debug("DeadlettersQueueReactiveGetHandler constructed!");
    }


    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        groupId = exchange.getQueryParameters().get("group")==null? config.getGroupId() : exchange.getQueryParameters().get("group").getFirst();

        instanceId = REPLAY_DEFAULT_INSTANCE;


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

        topics=topics.stream().map(t->t + config.getDeadLetterTopicExt()).collect(Collectors.toList());

        ConsumerSubscriptionRecord subscription = subscribeTopic(topics);

        exchange.dispatch();
        long recordsCount=0;
        int index=0;
        List<ConsumerRecord<Object, Object>> records;
        AtomicReference<Result<List<ConsumerRecord<Object, Object>>>> returnedResult = null;
        while(index <20 && recordsCount ==0 ) {
            returnedResult =readRecords(
                        exchange,
                        groupId,
                        instanceId,
                        Duration.ofMillis(timeoutMs),
                        subscription.getTopics(),
                        KafkaConsumerState.class,
                        SidecarConsumerRecord::fromConsumerRecord);

                if(!ObjectUtils.isEmpty(returnedResult) && !ObjectUtils.isEmpty(returnedResult.get()) && returnedResult.get().isSuccess() && !ObjectUtils.isEmpty(returnedResult.get().getResult())){
                    recordsCount= returnedResult.get().getResult().size();
                }
                else{
                    Thread.sleep(config.getWaitPeriod());
                }
//                System.out.println(index);

                index++;
        }
        if(ObjectUtils.isEmpty(returnedResult) || (!ObjectUtils.isEmpty(returnedResult.get()) && returnedResult.get().isSuccess() && ObjectUtils.isEmpty(returnedResult.get().getResult()))) {
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
            exchange.setStatusCode(200);
            DeadLetterQueueReplayResponse deadLetterQueueReplayResponse = new DeadLetterQueueReplayResponse();
            deadLetterQueueReplayResponse.setGroup(groupId);
            deadLetterQueueReplayResponse.setTopics(topics);
            deadLetterQueueReplayResponse.setInstance(instanceId);
            deadLetterQueueReplayResponse.setRecords(0L);
            deadLetterQueueReplayResponse.setDescription("Dead letter queue process successful to end, no records processed");
            exchange.getResponseSender().send(JsonMapper.toJson(deadLetterQueueReplayResponse));
        }
        else if(!ObjectUtils.isEmpty(returnedResult) && !ObjectUtils.isEmpty(returnedResult.get()) && returnedResult.get().isSuccess() && !ObjectUtils.isEmpty(returnedResult.get().getResult()) && returnedResult.get().getResult().size() !=0){
            records= returnedResult.get().getResult();
            if (logger.isDebugEnabled())
                logger.debug("polled records size = " + records.size());
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<ClientResponse> reference = new AtomicReference<>();
            try {
                ClientRequest request = new ClientRequest().setMethod(Methods.POST).setPath(config.getBackendApiPath());
                request.getRequestHeaders().put(Headers.CONTENT_TYPE, "application/json");
                request.getRequestHeaders().put(Headers.TRANSFER_ENCODING, "chunked");
                if(config.isBackendConnectionReset()) {
                    request.getRequestHeaders().put(Headers.CONNECTION, "close");
                }
                request.getRequestHeaders().put(Headers.HOST, "localhost");
                if (logger.isInfoEnabled()) logger.info("Send a batch to the backend API");

                ReactiveConsumerStartupHook.kafkaConsumerManager.getConnection().sendRequest(request, Http2Client.getInstance().createClientCallback(reference, latch, JsonMapper.toJson(records.stream().map(SidecarConsumerRecord::fromConsumerRecord).collect(Collectors.toList()))));
                latch.await();
                int statusCode = reference.get().getResponseCode();
                boolean consumerExitStatus=false;
                String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
                /**
                 * If consumer has exited by the time backend responds back,
                 * then create another subscription.
                 */
                if(null == ReactiveConsumerStartupHook.kafkaConsumerManager.getExistingConsumerInstance(groupId, instanceId) ||
                        null == ReactiveConsumerStartupHook.kafkaConsumerManager.getExistingConsumerInstance(groupId, instanceId).getId() ||
                        StringUtils.isEmpty(ReactiveConsumerStartupHook.kafkaConsumerManager.getExistingConsumerInstance(groupId, instanceId).getId().getInstance())){
                    subscribeTopic(topics);
                    consumerExitStatus=true;
                    logger.info("Resubscribed to topic as consumer had exited .");
                }
                if (logger.isDebugEnabled())
                    logger.debug("statusCode = " + statusCode + " body  = " + body);
                if (statusCode >= 400) {
                    // something happens on the backend and the data is not consumed correctly.
                    logger.error("Rollback due to error response from backend with status code = " + statusCode + " body = " + body);
                    ReactiveConsumerStartupHook.kafkaConsumerManager.rollback(records, groupId, instanceId);
                    ReactiveConsumerStartupHook.kafkaConsumerManager.rollbackExchangeDefinition(exchange,groupId, instanceId, topics, records);
                } else {
                    // The body will contains RecordProcessedResult for dead letter queue and audit.
                    // Write the dead letter queue if necessary.
                    if (logger.isInfoEnabled())
                        logger.info("Got successful response from the backend API");
                    processResponse(lightProducer, config, body, statusCode, records.size(), auditRecords);
                    /**
                     * If it is a new consumer , we need to seek to returned offset.
                     * If existing consumer instance, then commit offset.
                     */
                    if(consumerExitStatus){
                        ReactiveConsumerStartupHook.kafkaConsumerManager.seekToParticularOffset(records, groupId, instanceId);
                    }
                    else{
                        ReactiveConsumerStartupHook.kafkaConsumerManager.commitCurrentOffsets(groupId, instanceId);
                    }
                    if (logger.isDebugEnabled())
                        logger.debug("total dlq records processed:" + records.size());
                    ReactiveConsumerStartupHook.kafkaConsumerManager.successExchangeDefinition(exchange, groupId, instanceId, topics, records);

                }
            } catch (Exception exception) {
                logger.error("Rollback due to process response exception: ", exception);
                /**
                 * If consumer has exited by the time backend responds back,
                 * then create another subscription.
                 */
                if(null == ReactiveConsumerStartupHook.kafkaConsumerManager.getExistingConsumerInstance(groupId, instanceId) ||
                        null == ReactiveConsumerStartupHook.kafkaConsumerManager.getExistingConsumerInstance(groupId, instanceId).getId() ||
                        StringUtils.isEmpty(ReactiveConsumerStartupHook.kafkaConsumerManager.getExistingConsumerInstance(groupId, instanceId).getId().getInstance())){
                    subscribeTopic(topics);
                    logger.info("Resubscribed to topic as consumer had exited .");
                }
                ReactiveConsumerStartupHook.kafkaConsumerManager.rollback(records, groupId, instanceId);
                ReactiveConsumerStartupHook.kafkaConsumerManager.rollbackExchangeDefinition(exchange, groupId, instanceId, topics, records);
            }

        }
        else{
            setExchangeStatus(exchange, returnedResult.get().getError());
            return;
        }

    }

    private AtomicReference<Result<List<ConsumerRecord<Object, Object>>>> readRecords(
            HttpServerExchange exchange,
            String group,
            String instance,
            Duration timeout,
            List<String> topics,
            Class<KafkaConsumerState>
                    consumerStateType,
            Function<ConsumerRecord<Object, Object>, ?> toJsonWrapper
    ) {

        maxBytes = (maxBytes <= 0) ? Long.MAX_VALUE : maxBytes;
        try {
            ClientConnection connection = ReactiveConsumerStartupHook.kafkaConsumerManager.getConnection();
            if (connection != null && connection.isOpen()) {
                ReactiveConsumerStartupHook.kafkaConsumerManager.readRecords(
                        group, instance, consumerStateType, timeout, maxBytes,
                        new ConsumerReadCallback<Object, Object>() {
                            @Override
                            public void onCompletion(
                                    List<ConsumerRecord<Object, Object>> records, FrameworkException e
                            ) {
                                if (e != null) {
                                    logger.error("FrameworkException:", e);
                                    Status status = new Status(UNEXPECTED_CONSUMER_READ_EXCEPTION, e.getMessage());
                                    result.set(Failure.of(status));
                                } else {
                                    if (records.size() > 0) {
                                        result.set(Success.of(records));

                                        if (logger.isDebugEnabled())
                                            logger.debug("polled records size = " + records.size());

                                    }
                                    else{
                                        result.set(Success.of(null));
                                    }
                                }
                            }
                        }
                );
                return result;
            }
        }
        catch (Exception exc) {
            logger.info("Could not borrow backend connection , please retry !!!", exc);
        }
        return result;

    }

    public ConsumerSubscriptionRecord subscribeTopic(List<String> topics){
        KafkaConsumerState state = ReactiveConsumerStartupHook.kafkaConsumerManager.getExistingConsumerInstance(groupId, REPLAY_DEFAULT_INSTANCE);
        if (state == null) {
            CreateConsumerInstanceRequest request = new CreateConsumerInstanceRequest(REPLAY_DEFAULT_INSTANCE, null, config.getKeyFormat(), config.getValueFormat(), null, null, null, null);
            instanceId = ReactiveConsumerStartupHook.kafkaConsumerManager.createConsumer(groupId, request.toConsumerInstanceConfig());
        }
        ConsumerSubscriptionRecord subscription;
        subscription = new ConsumerSubscriptionRecord(topics, null);
        ReactiveConsumerStartupHook.kafkaConsumerManager.subscribe(groupId, instanceId, subscription);
        return subscription;
    }

}
