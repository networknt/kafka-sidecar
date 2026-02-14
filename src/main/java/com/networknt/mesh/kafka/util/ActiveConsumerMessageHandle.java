package com.networknt.mesh.kafka.util;

import com.networknt.mesh.kafka.ActiveConsumerStartupHook;
import com.networknt.mesh.kafka.WriteAuditLog;
import com.networknt.client.Http2Client;
import com.networknt.client.simplepool.SimpleConnectionState;
import com.networknt.config.Config;
import com.networknt.http.HttpStatus;
import com.networknt.kafka.common.config.KafkaConsumerConfig;
import com.networknt.kafka.consumer.KafkaConsumerManager;
import com.networknt.kafka.entity.*;
import com.networknt.kafka.producer.SidecarProducer;
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
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ActiveConsumerMessageHandle extends WriteAuditLog {

    private static final Logger logger= LoggerFactory.getLogger(ActiveConsumerMessageHandle.class);
    static final KafkaConsumerConfig consumerConfig= KafkaConsumerConfig.load();
    public static Http2Client client = Http2Client.getInstance();
    public static ClientConnection connection;
    public List<AuditRecord> auditRecords = new ArrayList<>();
    public static boolean firstBatch;

    public long listenOnMessage(String inputRecords, long currentOffset, TopicReplayMetadata topicReplayMetadata,
                                KafkaConsumerManager kafkaConsumerManager, String instanceId, SidecarProducer lightProducer){
        long start=System.currentTimeMillis();;
        List<Map<String,Object>> recordMaps=null;
        List<SidecarConsumerRecord> records=new ArrayList<>();
        SimpleConnectionState.ConnectionToken connectionToken = null;


        try{
            recordMaps= ConvertToList.string2List(Config.getInstance().getMapper(),inputRecords);
            logger.info("Parsed received replay records at {} , batch size is  {}", LocalDateTime.now(), recordMaps.size());
            AtomicLong offset = new AtomicLong(currentOffset);
            if(! ObjectUtils.isEmpty(recordMaps) && !recordMaps.isEmpty() ) {

                recordMaps.forEach((record) -> {
                    ConsumerRecord consumerRecord=Config.getInstance().getMapper().convertValue(record, ConsumerRecord.class);

                    /**
                     * This check is needed for the scenario when input start offset smaller than actual start offset
                     */
                    if(firstBatch && topicReplayMetadata.getStartOffset() !=consumerRecord.getOffset()){
                        throw new RuntimeException("For the first batch , start offset and first read offset do not match, possible error in input start offset, abort");
                    }
                    else{
                        firstBatch=false;
                    }

                    if(consumerRecord.getOffset() >= topicReplayMetadata.getStartOffset() && consumerRecord.getOffset() < topicReplayMetadata.getEndOffset()){
                        records.add(SidecarConsumerRecord.fromConsumerRecord(consumerRecord));
                        offset.set(consumerRecord.getOffset());
                    }
                });
            }
            else{
                return currentOffset+1;
            }

            /**
             * Invoke Backend only if records were added to be sent to backend
             */
            if(!ObjectUtils.isEmpty(records) && !records.isEmpty()) {
                if (connection == null || !connection.isOpen()) {
                    if (consumerConfig.getBackendApiHost().startsWith(Constants.HTTPS)) {
                        connectionToken = client.borrow(new URI(consumerConfig.getBackendApiHost()), Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true));
                    } else {
                        connectionToken = client.borrow(new URI(consumerConfig.getBackendApiHost()), Http2Client.WORKER, Http2Client.BUFFER_POOL, OptionMap.EMPTY);
                    }
                    connection = (ClientConnection) connectionToken.getRawConnection();
                }

                final AtomicReference<ClientResponse> reference = new AtomicReference<>();
                ClientRequest request = new ClientRequest().setMethod(Methods.POST).setPath(consumerConfig.getBackendApiPath());
                request.getRequestHeaders().put(Headers.CONTENT_TYPE, "application/json");
                request.getRequestHeaders().put(Headers.TRANSFER_ENCODING, "chunked");
                if (consumerConfig.getBackendConnectionReset()) {
                    request.getRequestHeaders().put(Headers.CONNECTION, "close");
                }
                request.getRequestHeaders().put(Headers.HOST, "localhost");
                if (logger.isInfoEnabled()) logger.info("Send a batch to the backend API, size {}", records.size());
                final CountDownLatch latch = new CountDownLatch(1);
                connection.sendRequest(request, client.createClientCallback(reference, latch, Config.getInstance().getMapper().writeValueAsString(records)));
                latch.await(consumerConfig.getInstanceTimeoutMs(), TimeUnit.MILLISECONDS);
                int statusCode = 0;
                String body = null;
                /**
                 * If we get timeout from backend API, throw exception and exit the stream processing silently and wait for next request.
                 * If we get 400 or more error code from backend API, throw exception and exit the stream processing silently and wait for next request.
                 * If we get response after timeout has occurred, then discard it
                 * If successful response, then continue further
                 */
                if (!ObjectUtils.isEmpty(reference) && !ObjectUtils.isEmpty(reference.get()) &&
                        !(reference.get().getResponseCode() >= HttpStatus.BAD_REQUEST.value())) {
                    statusCode = reference.get().getResponseCode();
                    body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);

                    if (logger.isInfoEnabled()) {
                        logger.info("Got successful response from the backend API");
                    }

                    /**
                     * If consumer has exited by the time backend responds back,
                     * then create another subscription.
                     * Although below piece of code looks redundant, leaving it here for a unfortunate racing situation between REST thread and consumer thread.
                     */
                    if (null == kafkaConsumerManager.getExistingConsumerInstance(topicReplayMetadata.getConsumerGroup(), instanceId) ||
                            null == kafkaConsumerManager.getExistingConsumerInstance(topicReplayMetadata.getConsumerGroup(), instanceId).getId() ||
                            StringUtils.isEmpty(kafkaConsumerManager.getExistingConsumerInstance(topicReplayMetadata.getConsumerGroup(), instanceId).getId().getInstance())) {
                        logger.info("Reinitiating the consumer subscription as consumer instance had died , increase instance time out MS or preferably reduce batch size ");
                        SubscribeTopic subscribeTopic = new SubscribeTopic(topicReplayMetadata.getConsumerGroup());
                        subscribeTopic.subscribeToTopic(topicReplayMetadata);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("statusCode = " + statusCode + " body  = " + body);
                    }

                    processResponse(ActiveConsumerStartupHook.kafkaConsumerManager, lightProducer, consumerConfig, body, statusCode, records.size(), auditRecords, topicReplayMetadata.isLastRetry());

                } else if (!ObjectUtils.isEmpty(reference) && !ObjectUtils.isEmpty(reference.get()) &&
                        reference.get().getResponseCode() >= HttpStatus.BAD_REQUEST.value()) {
                    logger.error("Received bad response from backendAPI call, status code {}, will throw exception and silently exit the processing.", reference.get().getResponseCode());
                    throw new RuntimeException("Received bad response from backendAPI call, status code " + reference.get().getResponseCode());
                } else {
                    logger.error("Timeout exception with backendAPI call , will throw exception and silently exit the processing.");
                    throw new RuntimeException("Timeout exception with backendAPI call");
                }

                /**
                 * Always seek/commit to the offset of last record in the processed batch for each topic and each partition
                 */

                TopicPartitionOffsetMetadata topicPartitionOffsetMetadata = new TopicPartitionOffsetMetadata(topicReplayMetadata.getTopicName(),
                        Integer.valueOf(topicReplayMetadata.getPartition()), offset.get(), null);
                List<TopicPartitionOffsetMetadata> topicPartitionOffsetMetadataList = Arrays.asList(topicPartitionOffsetMetadata);
                ConsumerOffsetCommitRequest consumerOffsetCommitRequest = new ConsumerOffsetCommitRequest(topicPartitionOffsetMetadataList);
                Future completableFuture = kafkaConsumerManager.commitOffsets(topicReplayMetadata.getConsumerGroup(), instanceId, false, consumerOffsetCommitRequest, (list, e1) -> {
                    if (null != e1) {
                        logger.error("Error committing offset, will force a restart ", e1);
                        throw new RuntimeException(e1.getMessage());
                    } else {
                        topicPartitionOffsetMetadataList.forEach((topicPartitionOffset -> {
                            logger.info("Committed to topic = " + topicPartitionOffset.getTopic() +
                                    " partition = " + topicPartitionOffset.getPartition() + " offset = " + topicPartitionOffset.getOffset());
                        }));
                    }
                });
                if (logger.isDebugEnabled()) {
                    logger.debug("time taken to process one batch and get response from backend in MS {} ", System.currentTimeMillis() - start);
                }
                /**
                 * Waiting for the future to complete before returning
                 */
                completableFuture.get();
                return offset.get()+1;
            }
            else{
                return currentOffset+1;
            }

        } catch(Exception e){
            throw new RuntimeException("ActiveConsumerMessageHandle exception while processing read message "+ e.getMessage());
        } finally {
            client.restore(connectionToken);
        }

    }

}
