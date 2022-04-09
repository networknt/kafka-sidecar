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
import com.networknt.utility.Constants;
import com.networknt.utility.ObjectUtils;
import com.networknt.utility.StringUtils;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.OptionMap;

import java.net.URI;
import java.nio.charset.StandardCharsets;
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
    public static ClientConnection connection;
    public static Http2Client client = Http2Client.getInstance();
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

                connection.sendRequest(request, client.createClientCallback(reference, latch, JsonMapper.toJson(records.stream().map(SidecarConsumerRecord::fromConsumerRecord).collect(Collectors.toList()))));
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
                    rollback(records, groupId, instanceId);
                    rollbackExchangeDefinition(exchange,topics, records);
                } else {
                    // The body will contains RecordProcessedResult for dead letter queue and audit.
                    // Write the dead letter queue if necessary.
                    if (logger.isInfoEnabled())
                        logger.info("Got successful response from the backend API");
                    processResponse(body, statusCode, records.size());
                    /**
                     * If it is a new consumer , we need to seek to returned offset.
                     * If existing consumer instance, then commit offset.
                     */
                    if(consumerExitStatus){
                        seekToParticularOffset(records, groupId, instanceId);
                    }
                    else{
                        ReactiveConsumerStartupHook.kafkaConsumerManager.commitCurrentOffsets(groupId, instanceId);
                    }
                    if (logger.isDebugEnabled())
                        logger.debug("total dlq records processed:" + records.size());
                    successExchangeDefinition(exchange, topics, records);

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
                rollback(records, groupId, instanceId);
                rollbackExchangeDefinition(exchange, topics, records);
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
            if (getConnection() != null && connection.isOpen()) {
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


    private void rollback(List<ConsumerRecord<Object, Object>> records, String groupId, String instanceId) {

        List<ConsumerSeekRequest.PartitionOffset> offsets=seekOffsetListUtility(records);
        offsets.stream().forEach((consumerOffset ->{
            logger.info("Rolling back to topic = " + consumerOffset.getTopic() + " partition = "+ consumerOffset.getPartition()+ " offset = "+ consumerOffset.getOffset());
        }));
        List<ConsumerSeekRequest.PartitionTimestamp> timestamps = new ArrayList<>();
        ConsumerSeekRequest consumerSeekRequest = new ConsumerSeekRequest(offsets, timestamps);
        ReactiveConsumerStartupHook.kafkaConsumerManager.seek(groupId, instanceId, consumerSeekRequest);
    }

    private void seekToParticularOffset(List<ConsumerRecord<Object, Object>> records, String groupId, String instanceId) {

        List<ConsumerSeekRequest.PartitionOffset> offsets=seekOffsetListUtility(records);
        offsets.stream().forEach((consumerOffset ->{
            logger.info("Seeking to topic = " + consumerOffset.getTopic() + " partition = "+ consumerOffset.getPartition()+ " offset = "+ consumerOffset.getOffset());
        }));
        List<ConsumerSeekRequest.PartitionTimestamp> timestamps = new ArrayList<>();
        ConsumerSeekRequest consumerSeekRequest = new ConsumerSeekRequest(offsets, timestamps);
        ReactiveConsumerStartupHook.kafkaConsumerManager.seek(groupId, instanceId, consumerSeekRequest);
    }

    private void processResponse(String responseBody, int statusCode, int recordSize) {

        if(responseBody != null) {
            long start = System.currentTimeMillis();
            List<Map<String, Object>> results = JsonMapper.string2List(responseBody);
            if (results.size() != recordSize) {
                // if the string2List failed, then a RuntimeException has thrown already.
                // https://github.com/networknt/kafka-sidecar/issues/70 if the response size doesn't match the record size
                logger.error("The response size " + results.size() + " does not match the record size " + recordSize);
                throw new RuntimeException("The response size " + results.size() + " does not match the record size " + recordSize);
            }
            for (int i = 0; i < results.size(); i++) {
                ObjectMapper objectMapper = Config.getInstance().getMapper();
                RecordProcessedResult result = objectMapper.convertValue(results.get(i), RecordProcessedResult.class);

                if (config.isDeadLetterEnabled() && !result.isProcessed()) {
                    try {
                        logger.info("Sending correlation id ::: "+ result.getCorrelationId() + " traceabilityId ::: "+ result.getTraceabilityId() + " to DLQ topic ::: "+ result.getRecord().getTopic());
                        ProduceRequest produceRequest = ProduceRequest.create(null, null, null, null,
                                null, null,null, null,null, null, null );
                        ProduceRecord produceRecord = ProduceRecord.create(null,null, null, null, null);
                        produceRecord.setKey(Optional.of(objectMapper.readTree(objectMapper.writeValueAsString(result.getRecord().getKey()))));
                        produceRecord.setValue(Optional.of(objectMapper.readTree(objectMapper.writeValueAsString(result.getRecord().getValue()))));
                        produceRecord.setCorrelationId(Optional.ofNullable(result.getCorrelationId()));
                        produceRecord.setTraceabilityId(Optional.ofNullable(result.getTraceabilityId()));
                        produceRequest.setRecords(Arrays.asList(produceRecord));
                        // populate the keyFormat and valueFormat from kafka-producer.yml if request doesn't have them.
                        if(config.getKeyFormat() != null) {
                            produceRequest.setKeyFormat(Optional.of(EmbeddedFormat.valueOf(config.getKeyFormat().toUpperCase())));
                        }
                        if(config.getValueFormat() != null) {
                            produceRequest.setValueFormat(Optional.of(EmbeddedFormat.valueOf(config.getValueFormat().toUpperCase())));
                        }
                        org.apache.kafka.common.header.Headers headers = populateHeaders(result);
                        CompletableFuture<ProduceResponse> responseFuture = lightProducer.produceWithSchema(result.getRecord().getTopic(), Server.getServerConfig().getServiceId(), Optional.empty(), produceRequest, headers, auditRecords);
                        responseFuture.whenCompleteAsync((response, throwable) -> {
                            // write the audit log here.
                            long startAudit = System.currentTimeMillis();
                            synchronized (auditRecords) {
                                if (auditRecords != null && auditRecords.size() > 0) {
                                    auditRecords.forEach(ar -> {
                                        writeAuditLog(ar, config.getAuditTarget(), config.getAuditTopic());
                                    });
                                    // clean up the audit entries
                                    auditRecords.clear();
                                }
                            }
                            if(logger.isDebugEnabled()) {
                                logger.debug("Writing audit log takes " + (System.currentTimeMillis() - startAudit));
                            }
                        });

                    }
                    catch(Exception e){
                        logger.error("Could not process record for traceability id ::: "+ result.getTraceabilityId() + ", correlation id ::: "+ result.getCorrelationId() + " to produce record for DLQ, will skip and proceed for next record ",e);

                    }
                }

                if(config.isAuditEnabled())  reactiveConsumerAuditLog(result, config.getAuditTarget(), config.getAuditTopic());
            }
            if(logger.isDebugEnabled()) {
                logger.debug("DeadlettersQueueReactiveGetHandler process response total time is " + (System.currentTimeMillis() - start));
            }
        }
        else {
            // https://github.com/networknt/kafka-sidecar/issues/70 to check developers errors.
            // throws exception if the body is empty when the response code is successful.
            logger.error("Response body is empty with success status code is " + statusCode);
            throw new RuntimeException("Response Body is empty with success status code " + statusCode);
        }

    }

    public ClientConnection getConnection() {

        if (connection == null || !connection.isOpen()) {
            try {
                if (config.getBackendApiHost().startsWith("https")) {
                    connection = client.borrowConnection(new URI(config.getBackendApiHost()), Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true)).get();
                } else {
                    connection = client.borrowConnection(new URI(config.getBackendApiHost()), Http2Client.WORKER, Http2Client.BUFFER_POOL, OptionMap.EMPTY).get();
                }
            } catch (Exception ex) {
                throw new RuntimeException();
            }
        }
        return connection;
    }


    public org.apache.kafka.common.header.Headers populateHeaders(RecordProcessedResult recordProcessedResult) {
        org.apache.kafka.common.header.Headers headers = new RecordHeaders();
        if (recordProcessedResult.getCorrelationId() != null) {
            headers.add(Constants.CORRELATION_ID_STRING, recordProcessedResult.getCorrelationId().getBytes(StandardCharsets.UTF_8));
        }
        if (recordProcessedResult.getTraceabilityId() != null) {
            headers.add(Constants.TRACEABILITY_ID_STRING, recordProcessedResult.getTraceabilityId().getBytes(StandardCharsets.UTF_8));
        }
        if (recordProcessedResult.getStacktrace() != null) {
            headers.add(Constants.STACK_TRACE, recordProcessedResult.getStacktrace().getBytes(StandardCharsets.UTF_8));
        }
        Map<String, String> recordHeaders = recordProcessedResult.getRecord().getHeaders();
        if (recordHeaders != null && recordHeaders.size() > 0) {
            recordHeaders.keySet().stream().forEach(h -> {
                if (recordHeaders.get(h) != null) {
                    headers.add(h, recordHeaders.get(h).getBytes(StandardCharsets.UTF_8));
                }
            });
        }

        return headers;
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

    public List<ConsumerSeekRequest.PartitionOffset> seekOffsetListUtility(List<ConsumerRecord<Object, Object>> records){

        // as one topic multiple partitions or multiple topics records will be in the same list, we need to find out how many offsets that is need to seek.
        Map<String, ConsumerSeekRequest.PartitionOffset> topicPartitionMap = new HashMap<>();
        for(ConsumerRecord record: records) {
            String topic = record.getTopic();
            int partition = record.getPartition();
            long offset = record.getOffset();
            ConsumerSeekRequest.PartitionOffset partitionOffset = topicPartitionMap.get(topic + ":" + partition);
            if(partitionOffset == null) {
                partitionOffset = new ConsumerSeekRequest.PartitionOffset(topic, partition, offset, null);
                topicPartitionMap.put(topic + ":" + partition, partitionOffset);
            } else {
                // found the record in the map, set the offset if the current offset is smaller.
                if(partitionOffset.getOffset() > offset) {
                    partitionOffset.setOffset(offset);
                }
            }
        }
        // convert the map values to a list.
        List<ConsumerSeekRequest.PartitionOffset> offsets = topicPartitionMap.values().stream()
                .collect(Collectors.toList());
        return offsets;

    }

    public void rollbackExchangeDefinition(HttpServerExchange exchange, List<String> topics, List<ConsumerRecord<Object, Object>> records){
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.setStatusCode(200);
        DeadLetterQueueReplayResponse deadLetterQueueReplayResponse = new DeadLetterQueueReplayResponse();
        deadLetterQueueReplayResponse.setGroup(groupId);
        deadLetterQueueReplayResponse.setTopics(topics);
        deadLetterQueueReplayResponse.setInstance(instanceId);
        deadLetterQueueReplayResponse.setRecords(Long.valueOf(records.size()));
        ConsumerRecord<Object, Object> record = records.get(records.size()-1);
        deadLetterQueueReplayResponse.setDescription("Pulled records from DLQ , processing error, rolled back to partition:" + record.getPartition() +  "| offset:" + record.getOffset());
        exchange.getResponseSender().send(JsonMapper.toJson(deadLetterQueueReplayResponse));
    }

    public void successExchangeDefinition(HttpServerExchange exchange, List<String> topics, List<ConsumerRecord<Object, Object>> records){
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.setStatusCode(200);
        DeadLetterQueueReplayResponse deadLetterQueueReplayResponse = new DeadLetterQueueReplayResponse();
        deadLetterQueueReplayResponse.setGroup(groupId);
        deadLetterQueueReplayResponse.setTopics(topics);
        deadLetterQueueReplayResponse.setInstance(instanceId);
        deadLetterQueueReplayResponse.setRecords(Long.valueOf(records.size()));
        ConsumerRecord<Object, Object> record = records.get(records.size()-1);
        deadLetterQueueReplayResponse.setDescription("Dead letter queue process successful to partition:" + record.getPartition() +  "| offset:" + record.getOffset());
        exchange.getResponseSender().send(JsonMapper.toJson(deadLetterQueueReplayResponse));
    }
}
