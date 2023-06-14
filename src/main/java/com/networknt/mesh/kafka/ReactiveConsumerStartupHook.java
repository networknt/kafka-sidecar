package com.networknt.mesh.kafka;

import com.networknt.client.simplepool.SimpleConnectionHolder;
import com.networknt.kafka.producer.NativeLightProducer;
import com.networknt.kafka.producer.SidecarProducer;
import com.networknt.client.Http2Client;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.exception.FrameworkException;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.consumer.*;
import com.networknt.kafka.entity.*;
import com.networknt.server.StartupHookProvider;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.utility.Constants;
import com.networknt.utility.ObjectUtils;
import com.networknt.utility.StringUtils;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.OptionMap;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * This startup hook depending on the ProducerStartupHook to produce the dead letters to the Kafka dead letter topic.
 * So we need to make sure that the ProducerStartupHook is configured in the service.yml file. Otherwise, an error
 * will be thrown.
 *
 */
public class ReactiveConsumerStartupHook extends WriteAuditLog implements StartupHookProvider {
    private static Logger logger = LoggerFactory.getLogger(ReactiveConsumerStartupHook.class);
    public static KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    public static KafkaConsumerManager kafkaConsumerManager;
    public static boolean healthy = true;
    public static Http2Client client = Http2Client.getInstance();
    static private ExecutorService executor = newSingleThreadExecutor();
    public List<AuditRecord> auditRecords = new ArrayList<>();
    long timeoutMs = -1;
    long maxBytes = -1;
    String instanceId;
    String groupId;
    // An indicator that will break the consumer loop so that the consume can be closed. It is set
    // by the ReactiveConsumerShutdownHook to do the clean up.
    public static boolean done = false;
    // send the next batch only when the response for the previous batch is returned.
    public static boolean readyForNextBatch = false;
    SidecarProducer lightProducer;

    @Override
    public void onStartup() {
        logger.info("ReactiveConsumerStartupHook begins");
        if(config.isDeadLetterEnabled()) {
            if (ProducerStartupHook.producer != null) {
                lightProducer = (SidecarProducer) SingletonServiceFactory.getBean(NativeLightProducer.class);
            } else {
                logger.error("ProducerStartupHook is not configured and it is needed if DLQ is enabled");
                throw new RuntimeException("ProducerStartupHook is not loaded!");
            }
        }
        // get or create the KafkaConsumerManager
        kafkaConsumerManager = new KafkaConsumerManager(config);
        groupId = (String) config.getProperties().get("group.id");
        subscribeTopic();
        runConsumer();
        logger.info("ReactiveConsumerStartupHook ends");
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
                readRecords(
                        KafkaConsumerState.class,
                        SidecarConsumerRecord::fromConsumerRecord);

                while (!readyForNextBatch) {
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
            Class<KafkaConsumerState>
                    consumerStateType,
            Function<com.networknt.kafka.entity.ConsumerRecord<ClientKeyT, ClientValueT>, ?> toJsonWrapper
    ) {
        maxBytes = (maxBytes <= 0) ? Long.MAX_VALUE : maxBytes;
        Duration timeout = Duration.ofMillis(timeoutMs);
        SimpleConnectionHolder.ConnectionToken connectionToken = null;
        try {
            if (config.getBackendApiHost().startsWith(Constants.HTTPS)) {
                connectionToken = client.borrow(new URI(config.getBackendApiHost()), Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true));
            } else {
                connectionToken = client.borrow(new URI(config.getBackendApiHost()), Http2Client.WORKER, Http2Client.BUFFER_POOL, OptionMap.EMPTY);
            }
            ClientConnection connection = (ClientConnection) connectionToken.getRawConnection();
            final SimpleConnectionHolder holder = connectionToken.holder();
            /**
             * Scenario to handle when consumer goes away but container does not die.
             */
            if(null == kafkaConsumerManager.getExistingConsumerInstance(groupId, instanceId) ||
                    null == kafkaConsumerManager.getExistingConsumerInstance(groupId, instanceId).getId() ||
                    StringUtils.isEmpty(kafkaConsumerManager.getExistingConsumerInstance(groupId, instanceId).getId().getInstance())){
                healthy = false;
                logger.error("Consumer instance not found, marking health as false for group id: " , groupId);
            }
            kafkaConsumerManager.readRecords(
                    groupId, instanceId, consumerStateType, timeout, maxBytes,
                    new ConsumerReadCallback<ClientKeyT, ClientValueT>() {
                        @Override
                        public void onCompletion(
                                List<ConsumerRecord<ClientKeyT, ClientValueT>> records, FrameworkException e
                        ) {
                            if (e != null) {
                                logger.error("FrameworkException: ", e);
                                // set active consumer healthy to false in order to restart the container/pod.
                                healthy = false;
                            } else {
                                // reset the healthy status to true if onCompletion returns data without exception for another try.
                                // this will ensure that k8s probe won't restart the pod by only one exception. It will only restart
                                // the pod when there are multiple health check failures in a row.
                                if (!healthy) healthy = true;
                                if (records.size() > 0) {
                                    if (logger.isDebugEnabled())
                                        logger.debug("polled records size = " + records.size());
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
                                        final CountDownLatch latch = new CountDownLatch(1);
                                        connection.sendRequest(request, client.createClientCallback(reference, latch, JsonMapper.toJson(records.stream().map(toJsonWrapper).collect(Collectors.toList()))));
                                        latch.await(config.getInstanceTimeoutMs(), TimeUnit.MILLISECONDS);
                                        int statusCode=0;
                                        if(!ObjectUtils.isEmpty(reference) && !ObjectUtils.isEmpty(reference.get())){
                                            statusCode = reference.get().getResponseCode();
                                        }
                                        else{
                                            throw new TimeoutException("Rest Call to backend latch timeout");
                                        }
                                        String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
                                        /**
                                         * If consumer has exited by the time backend responds back,
                                         * then create another subscription.
                                         * Although below piece of code looks redundant, leaving it here for a unfortunate racing situation between REST thread and consumer thread.
                                         */
                                        if(null == kafkaConsumerManager.getExistingConsumerInstance(groupId, instanceId) ||
                                                null == kafkaConsumerManager.getExistingConsumerInstance(groupId, instanceId).getId() ||
                                                StringUtils.isEmpty(kafkaConsumerManager.getExistingConsumerInstance(groupId, instanceId).getId().getInstance())){
                                            logger.info("Marking health status false as consumer had exited , increase instance time out MS or preferably reduce batch size");
                                            healthy=false;
                                            readyForNextBatch = false;
                                            return;
                                        }
                                        if (logger.isDebugEnabled())
                                            logger.debug("statusCode = " + statusCode + " body  = " + body);
                                        if (statusCode >= 400) {
                                            // something happens on the backend and the data is not consumed correctly.
                                            logger.error("Rollback due to error response from backend with status code = " + statusCode + " body = " + body);
                                            kafkaConsumerManager.rollback(records, groupId, instanceId);
                                            readyForNextBatch = true;
                                        } else {
                                            // The body will contains RecordProcessedResult for dead letter queue and audit.
                                            // Write the dead letter queue if necessary.
                                            if (logger.isInfoEnabled())
                                                logger.info("Got successful response from the backend API");
                                            processResponse(lightProducer, config, body, statusCode, records.size(), auditRecords, false);
                                            /**
                                             * Always seek to the offset of last record in the processed batch for each topic and each partition
                                             */

                                            List<TopicPartitionOffsetMetadata> topicPartitionOffsetMetadataList= topicPartitionOffsetMetadataUtility(records);
                                            ConsumerOffsetCommitRequest consumerOffsetCommitRequest= new ConsumerOffsetCommitRequest(topicPartitionOffsetMetadataList);
                                            kafkaConsumerManager.commitOffsets(groupId, instanceId, false, consumerOffsetCommitRequest, (list, e1) -> {
                                                if(null !=e1){
                                                    logger.error("Error committing offset, will force a restart ", e1);
                                                    throw new RuntimeException(e1.getMessage());
                                                }
                                                else{
                                                    topicPartitionOffsetMetadataList.forEach((topicPartitionOffset -> {
                                                        logger.info("Committed to topic = "+ topicPartitionOffset.getTopic() +
                                                                " partition = "+ topicPartitionOffset.getPartition()+ " offset = "+topicPartitionOffset.getOffset());
                                                    }));
                                                }
                                            });

                                            readyForNextBatch = true;
                                        }
                                    } catch (Exception exception) {
                                        logger.error("Process response exception: ", exception);
                                        // For spring boot backend, the connection created during the liveness and readiness won't work, need to close it to recreate.
                                        holder.safeClose(System.currentTimeMillis());
                                        logger.info("Marking health status false as consumer had exited , increase instance time out MS or preferably reduce batch size");
                                        healthy=false;
                                        readyForNextBatch = false;
                                        return;
                                    }
                                } else {
                                    // Record size is zero. Do we need an extra period of sleep?
                                    if (logger.isTraceEnabled())
                                        logger.trace("Polled nothing from the Kafka cluster or connection to backend is null");

                                    readyForNextBatch = true;
                                }
                            }
                        }
                    }
            );
        } catch (Exception exc) {
            logger.info("Could not communicate with backend, will retry after sleeping!!!", exc);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            readyForNextBatch = true;
        } finally {
            client.restore(connectionToken);
        }
    }

    public void subscribeTopic(){
        CreateConsumerInstanceRequest request = new CreateConsumerInstanceRequest(null, null, config.getKeyFormat(), config.getValueFormat(), null, null, null, null);
        instanceId = kafkaConsumerManager.createConsumer(groupId, request.toConsumerInstanceConfig());

        String topic = config.getTopic();
        ConsumerSubscriptionRecord subscription;
        if (topic.contains(",")) {
            // remove the whitespaces
            topic = topic.replaceAll("\\s+", "");
            subscription = new ConsumerSubscriptionRecord(Arrays.asList(topic.split(",", -1)), null);
        } else {
            subscription = new ConsumerSubscriptionRecord(Collections.singletonList(config.getTopic()), null);
        }
        kafkaConsumerManager.subscribe(groupId, instanceId, subscription);
    }

    public static <ClientKeyT, ClientValueT> List<TopicPartitionOffsetMetadata> topicPartitionOffsetMetadataUtility(List<ConsumerRecord<ClientKeyT, ClientValueT>> records){
    // as one topic multiple partitions or multiple topics records will be in the same list, we need to find out how many offsets that we need to commit.
        Map<String, TopicPartitionOffsetMetadata> topicPartitionMap = new HashMap<>();
        for(ConsumerRecord record: records) {
            String topic = record.getTopic();
            int partition = record.getPartition();
            long offset = record.getOffset();
            TopicPartitionOffsetMetadata partitionOffset = topicPartitionMap.get(topic + ":" + partition);
            if(partitionOffset == null) {
                partitionOffset = new TopicPartitionOffsetMetadata(topic, partition, offset, null);
                topicPartitionMap.put(topic + ":" + partition, partitionOffset);
            } else {
                // found the record in the map, set the offset if the current offset is smaller.
                if(partitionOffset.getOffset() < offset) {
                    partitionOffset = new TopicPartitionOffsetMetadata(topic, partition, offset, null);
                    topicPartitionMap.put(topic + ":" + partition, partitionOffset);
                }
            }
        }
        // convert the map values to a list.
        List<TopicPartitionOffsetMetadata> offsets = topicPartitionMap.values().stream()
                .collect(Collectors.toList());
        return offsets;
    }

}
