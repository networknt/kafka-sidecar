package com.networknt.mesh.kafka;

import com.networknt.client.Http2Client;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.exception.FrameworkException;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.consumer.*;
import com.networknt.kafka.entity.*;
import com.networknt.server.Server;
import com.networknt.server.StartupHookProvider;
import com.networknt.utility.ModuleRegistry;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class ReactiveConsumerStartupHook implements StartupHookProvider {
    private static Logger logger = LoggerFactory.getLogger(ReactiveConsumerStartupHook.class);
    public static KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    public static KafkaConsumerManager kafkaConsumerManager;
    public static Http2Client client = Http2Client.getInstance();
    public static ClientConnection connection;
    static private ExecutorService executor = newSingleThreadExecutor();
    long timeoutMs = -1;
    long maxBytes = -1;
    String instanceId;
    String groupId;
    // An indicator that will break the consumer loop so that the consume can be closed. It is set
    // by the ReactiveConsumerShutdownHook to do the clean up.
    public static boolean done = false;
    // send the next batch only when the response for the previous batch is returned.
    public static boolean readyForNextBatch = false;

    @Override
    public void onStartup() {
        logger.debug("ReactiveConsumerStartupHook begins");
        // get or create the KafkaConsumerManager
        kafkaConsumerManager = new KafkaConsumerManager(config);
        groupId = (String)config.getProperties().get("group.id");
        CreateConsumerInstanceRequest request = new CreateConsumerInstanceRequest(null, null, config.getValueFormat(), null, null, null, null);
        instanceId = kafkaConsumerManager.createConsumer(groupId, request.toConsumerInstanceConfig());

        String topic = config.getTopic();
        ConsumerSubscriptionRecord subscription;
        if(topic.contains(",")) {
            // remove the whitespaces
            topic = topic.replaceAll("\\s+","");
            subscription = new ConsumerSubscriptionRecord(Arrays.asList(topic.split(",", -1)), null);
        } else {
            subscription = new ConsumerSubscriptionRecord(Collections.singletonList(config.getTopic()), null);
        }
        kafkaConsumerManager.subscribe(groupId, instanceId, subscription);
        runConsumer();
        List<String> masks = new ArrayList<>();
        masks.add("basic.auth.user.info");
        masks.add("sasl.jaas.config");
        ModuleRegistry.registerModule(ReactiveConsumerStartupHook.class.getName(), Config.getInstance().getJsonMapConfigNoCache(KafkaConsumerConfig.CONFIG_NAME), masks);
        logger.debug("ReactiveConsumerStartupHook ends");
    }

    private void runConsumer() {
        executor.execute(new ConsumerTask());
        executor.shutdown();
    }

    class ConsumerTask implements Runnable {
        @Override
        public void run() {
            while (!done) {
                readyForNextBatch = false;
                switch(config.getValueFormat()) {
                    case "binary":
                        readRecords(
                                BinaryKafkaConsumerState.class,
                                BinaryConsumerRecord::fromConsumerRecord);
                        break;
                    case "json":
                        readRecords(
                                JsonKafkaConsumerState.class,
                                JsonConsumerRecord::fromConsumerRecord);
                        break;
                    case "avro":
                    case "jsonschema":
                    case "protobuf":
                        readRecords(
                                SchemaKafkaConsumerState.class,
                                SchemaConsumerRecord::fromConsumerRecord);
                        break;
                }
                while(!readyForNextBatch) {
                    // wait until the previous batch returns.
                    try {
                        // wait a period of time before the next poll if there is no record
                        Thread.sleep(config.getWaitPeriod());
                    } catch (InterruptedException e) {
                        logger.error("InterruptedException", e);
                        // ignore it.
                    }
                }
            }
        }
    }

    private <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> void readRecords(
            Class<? extends KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>>
                    consumerStateType,
            Function<com.networknt.kafka.entity.ConsumerRecord<ClientKeyT, ClientValueT>, ?> toJsonWrapper
    ) {
        maxBytes = (maxBytes <= 0) ? Long.MAX_VALUE : maxBytes;
        Duration timeout = Duration.ofMillis(timeoutMs);
        kafkaConsumerManager.readRecords(
                groupId, instanceId, consumerStateType, timeout, maxBytes,
                new ConsumerReadCallback<ClientKeyT, ClientValueT>() {
                    @Override
                    public void onCompletion(
                            List<ConsumerRecord<ClientKeyT, ClientValueT>> records, FrameworkException e
                    ) {
                        if (e != null) {
                            if(logger.isDebugEnabled()) logger.debug("FrameworkException:", e);
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
                                        rollback(records.get(0));
                                        readyForNextBatch = true;
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
                                        rollback(records.get(0));
                                        readyForNextBatch = true;
                                    } else {
                                        // The body will contains RecordProcessedResult for dead letter queue and audit.
                                        // Write the dead letter queue if necessary.
                                        if(logger.isInfoEnabled()) logger.info("Got successful response from the backend API");
                                        processResponse(body);
                                        // commit the batch offset here.
                                        kafkaConsumerManager.commitCurrentOffsets(groupId, instanceId);
                                        readyForNextBatch = true;
                                    }
                                } catch (Exception exception) {
                                    logger.error("Rollback due to process response exception: ", exception);
                                    rollback(records.get(0));
                                    readyForNextBatch = true;
                                }
                            } else {
                                // Record size is zero. Do we need an extra period of sleep?
                                if(logger.isTraceEnabled()) logger.trace("poll nothing from the Kafka cluster");
                                readyForNextBatch = true;
                            }
                        }
                    }
                }
        );
    }

    private void rollback(ConsumerRecord firstRecord) {
        ConsumerSeekRequest.PartitionOffset partitionOffset = new ConsumerSeekRequest.PartitionOffset(firstRecord.getTopic(), firstRecord.getPartition(), firstRecord.getOffset(), null);
        List<ConsumerSeekRequest.PartitionOffset> offsets = new ArrayList<>();
        offsets.add(partitionOffset);
        List<ConsumerSeekRequest.PartitionTimestamp> timestamps = new ArrayList<>();
        ConsumerSeekRequest consumerSeekRequest = new ConsumerSeekRequest(offsets, timestamps);
        kafkaConsumerManager.seek(groupId, instanceId, consumerSeekRequest);
        if(logger.isDebugEnabled()) logger.debug("Rollback to topic " + firstRecord.getTopic() + " partition " + firstRecord.getPartition() + " offset " + firstRecord.getOffset());
    }

    private void processResponse(String responseBody) {
         if(responseBody != null) {
             List<Map<String, Object>> results = JsonMapper.string2List(responseBody);
             for(int i = 0; i < results.size(); i ++) {
                 RecordProcessedResult result = Config.getInstance().getMapper().convertValue(results.get(i), RecordProcessedResult.class);
                 if(config.isDeadLetterEnabled() && !result.isProcessed()) {
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
                                     if(logger.isTraceEnabled()) logger.trace("Write to dead letter topic meta " + metadata.topic() + " " + metadata.partition() + " " + metadata.offset());
                                 }
                             });
                 }
                 if(config.isAuditEnabled()) writeAuditLog(result);
             }
         }

    }

    private void writeAuditLog(RecordProcessedResult result) {
        AuditRecord auditRecord = new AuditRecord();
        auditRecord.setId(UUID.randomUUID().toString());
        auditRecord.setServiceId(Server.getServerConfig().getServiceId());
        auditRecord.setAuditType(AuditRecord.AuditType.REACTIVE_CONSUMER);
        auditRecord.setTopic(result.getRecord().getTopic());
        auditRecord.setPartition(result.getRecord().getPartition());
        auditRecord.setOffset(result.getRecord().getOffset());
        auditRecord.setCorrelationId((String)result.getRecord().getHeaders().get("X-Correlation-Id"));
        auditRecord.setTraceabilityId((String)result.getRecord().getHeaders().get("X-Traceability-Id"));
        auditRecord.setAuditStatus(result.isProcessed() ? AuditRecord.AuditStatus.SUCCESS : AuditRecord.AuditStatus.FAILURE);
        if(KafkaConsumerConfig.AUDIT_TARGET_TOPIC.equals(config.getAuditTarget())) {
            ProducerStartupHook.producer.send(
                    new ProducerRecord<>(
                            config.getAuditTopic(),
                            null,
                            System.currentTimeMillis(),
                            auditRecord.getCorrelationId().getBytes(StandardCharsets.UTF_8),
                            JsonMapper.toJson(auditRecord).getBytes(StandardCharsets.UTF_8),
                            null),
                    (metadata, exception) -> {
                        if (exception != null) {
                            // handle the exception by logging an error;
                            logger.error("Exception:" + exception);
                        } else {
                            if(logger.isTraceEnabled()) logger.trace("Write to audit topic meta " + metadata.topic() + " " + metadata.partition() + " " + metadata.offset());
                        }
                    });
        } else {
            SidecarAuditHelper.logResult(auditRecord);
        }
    }
}
