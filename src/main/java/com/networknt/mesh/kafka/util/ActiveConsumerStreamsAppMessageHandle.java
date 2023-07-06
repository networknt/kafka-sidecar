package com.networknt.mesh.kafka.util;

import com.networknt.mesh.kafka.WriteAuditLog;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.client.Http2Client;
import com.networknt.config.Config;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.consumer.KafkaConsumerManager;
import com.networknt.kafka.entity.*;
import com.networknt.kafka.producer.SidecarProducer;
import com.networknt.server.Server;
import com.networknt.utility.Constants;
import com.networknt.utility.ObjectUtils;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class ActiveConsumerStreamsAppMessageHandle extends WriteAuditLog {

    private static final Logger logger= LoggerFactory.getLogger(ActiveConsumerStreamsAppMessageHandle.class);
    static final KafkaConsumerConfig consumerConfig= (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    private static Http2Client client = Http2Client.getInstance();
    private static HttpClient httpClient = HttpClient.newBuilder().build();
    public List<AuditRecord> auditRecords = new ArrayList<>();
    public static boolean firstBatch;

    public long listenOnMessage(SidecarProducer lightProducer, String inputRecords , long currentOffset, TopicReplayMetadata topicReplayMetadata,
                                KafkaConsumerManager kafkaConsumerManager, String instanceId){

        long start=System.currentTimeMillis();;
        List<Map<String,Object>> recordMaps=null;
        ObjectMapper objectMapper= Config.getInstance().getMapper();



        try{
            recordMaps= ConvertToList.string2List(Config.getInstance().getMapper(),inputRecords);
            logger.info("Parsed received replay records at {} , batch size is  {}", LocalDateTime.now(), recordMaps.size());
            AtomicLong offset = new AtomicLong(currentOffset);
            ProduceRequest produceRequest= new ProduceRequest(Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    null);
            produceRequest.setKeyFormat(Optional.of(EmbeddedFormat.valueOf(consumerConfig.getKeyFormat().toUpperCase())));
            produceRequest.setValueFormat(Optional.of(EmbeddedFormat.valueOf(consumerConfig.getValueFormat().toUpperCase())));
            if(! ObjectUtils.isEmpty(recordMaps) && !recordMaps.isEmpty() ) {

                ArrayList<ProduceRecord> produceRecordsList = new ArrayList<>();
                recordMaps.forEach((record) -> {
                    Map<String, String> headers = (Map<String, String>) record.get("headers");
                    ProduceRecord produceRecord = ProduceRecord.create(null,null, null, null, null);
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
                    try {
                        produceRecord.setKey(Optional.of(objectMapper.readTree(objectMapper.writeValueAsString(consumerRecord.getKey()))));
                        produceRecord.setValue(Optional.of(objectMapper.readTree(objectMapper.writeValueAsString(consumerRecord.getValue()))));
                        if (!ObjectUtils.isEmpty(headers) && !ObjectUtils.isEmpty(headers.get(Constants.TRACEABILITY_ID_STRING))) {
                            produceRecord.setTraceabilityId(Optional.ofNullable(headers.get(Constants.TRACEABILITY_ID_STRING)));
                        }
                        if (!ObjectUtils.isEmpty(headers) && !ObjectUtils.isEmpty(headers.get(Constants.CORRELATION_ID_STRING))) {
                            produceRecord.setCorrelationId(Optional.ofNullable(headers.get(Constants.CORRELATION_ID_STRING)));
                        }
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("ActiveConsumerStreamsAppMessageHandle exception while parsing read message ", e);
                    }



                    produceRecordsList.add(produceRecord);
                    offset.set(consumerRecord.getOffset());
                    }

                });
                produceRequest.setRecords(produceRecordsList);
            }
            else{
                return currentOffset+1;
            }

            /**
             * Starting to send the list of records to destination topic
             */

            if (logger.isDebugEnabled()) {
                logger.debug("Forwarding message ::: {} to sidecar at ::: {}", objectMapper.writeValueAsString(produceRequest), LocalDateTime.now());
            }

            if(!ObjectUtils.isEmpty(produceRequest) && !produceRequest.getRecords().isEmpty()) {
                if (logger.isInfoEnabled()) logger.info("Send a batch to the destination topic API, size {}", produceRequest.getRecords().size());
                try {
                    CompletableFuture<ProduceResponse> responseFuture = lightProducer.produceWithSchema(topicReplayMetadata.getDestinationTopic(), Server.getServerConfig().getServiceId(), Optional.empty(), produceRequest, new RecordHeaders(), auditRecords);
                    responseFuture.whenCompleteAsync((response, throwable) -> {
                        // write the audit log here.
                        long startAudit = System.currentTimeMillis();
                        synchronized (auditRecords) {
                            if (auditRecords != null && auditRecords.size() > 0) {
                                auditRecords.forEach(ar -> {
                                    writeAuditLog(ar, consumerConfig.getAuditTarget(), consumerConfig.getAuditTopic());
                                });
                                // clean up the audit entries
                                auditRecords.clear();
                            }
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug("Writing audit log takes " + (System.currentTimeMillis() - startAudit));
                            logger.debug("ProducerTopicPostHandler handleRequest total time is " + (System.currentTimeMillis() - start));
                        }
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
            else{
                return currentOffset+1;
            }

            /**
             * We can not do anything with this response. If any produce fails to go to destination topic, then that will be recorded in audit.
             * We will just commit and move forward
             */

            if (logger.isDebugEnabled()) {
                logger.debug("Forwarded transformed message to sidecar at ::: {}", LocalDateTime.now());
            }

            /**
             * Always seek/commit to the offset of last record in the processed batch for each topic and each partition
             */

            TopicPartitionOffsetMetadata topicPartitionOffsetMetadata = new TopicPartitionOffsetMetadata(topicReplayMetadata.getTopicName(),
                    Integer.valueOf(topicReplayMetadata.getPartition()), offset.get(), null);
            List<TopicPartitionOffsetMetadata> topicPartitionOffsetMetadataList = Arrays.asList(topicPartitionOffsetMetadata);
            ConsumerOffsetCommitRequest consumerOffsetCommitRequest = new ConsumerOffsetCommitRequest(topicPartitionOffsetMetadataList);
            Future completableFuture=kafkaConsumerManager.commitOffsets(topicReplayMetadata.getConsumerGroup(), instanceId, false, consumerOffsetCommitRequest, (list, e1) -> {
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
        catch(Exception e){
            throw new RuntimeException("ActiveConsumerStreamsAppMessageHandle exception while processing read message "+ e.getMessage());
        }
    }
}
