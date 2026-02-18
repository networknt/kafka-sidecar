package com.networknt.mesh.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.kafka.common.config.KafkaConsumerConfig;
import com.networknt.kafka.consumer.KafkaConsumerManager;
import com.networknt.kafka.entity.*;
import com.networknt.kafka.producer.SidecarProducer;
import com.networknt.server.ServerConfig;
import com.networknt.utility.Constants;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.networknt.handler.LightHttpHandler.logger;

public class WriteAuditLog {
    private static final Logger logger = LoggerFactory.getLogger(WriteAuditLog.class);

    protected void activeConsumerAuditLog(RecordProcessedResult result, String auditTarget, String auditTopic) {
        writeAuditLog(auditFromRecordProcessedResult(result, AuditRecord.AuditType.ACTIVE_CONSUMER), auditTarget, auditTopic);
    }

    protected void reactiveConsumerAuditLog(RecordProcessedResult result, String auditTarget, String auditTopic) {
        writeAuditLog(auditFromRecordProcessedResult(result,AuditRecord.AuditType.REACTIVE_CONSUMER), auditTarget, auditTopic);
    }

    protected AuditRecord auditFromRecordProcessedResult(RecordProcessedResult result, AuditRecord.AuditType auditType) {
        AuditRecord auditRecord = new AuditRecord();
        auditRecord.setId(UUID.randomUUID().toString());
        auditRecord.setServiceId(ServerConfig.getInstance().getServiceId());
        auditRecord.setAuditType(auditType);
        auditRecord.setTopic(result.getRecord().getTopic());
        auditRecord.setPartition(result.getRecord().getPartition());
        auditRecord.setOffset(result.getRecord().getOffset());
        String correlationId = null;
        String traceabilityId = null;
        Map<String, String> headers = result.getRecord().getHeaders();
        if(headers != null) {
            correlationId = headers.get(Constants.CORRELATION_ID_STRING);
            if(correlationId == null) correlationId = result.getCorrelationId();
            traceabilityId = headers.get(Constants.TRACEABILITY_ID_STRING);
            if(traceabilityId == null) traceabilityId = result.getTraceabilityId();
        } else {
            correlationId = result.getCorrelationId();
            traceabilityId = result.getTraceabilityId();
        }
        auditRecord.setCorrelationId(correlationId);
        auditRecord.setTraceabilityId(traceabilityId);
        auditRecord.setKey(result.getKey());
        auditRecord.setTimestamp(result.getTimestamp());
        auditRecord.setAuditStatus(result.isProcessed() ? AuditRecord.AuditStatus.SUCCESS : AuditRecord.AuditStatus.FAILURE);
        return auditRecord;
    }

    protected void writeAuditLog(AuditRecord auditRecord, String auditTarget, String auditTopic) {
        if(KafkaConsumerConfig.AUDIT_TARGET_TOPIC.equals(auditTarget)) {
            // since both the traceabilityId and original message key can be empty, we are using the correlationId as the key
            // the correlationId won't be null as it will be created in that case for each record.
            AuditProducerStartupHook.auditProducer.send(
                    new ProducerRecord<>(
                            auditTopic,
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

    public void processResponse(KafkaConsumerManager kafkaConsumerManager, SidecarProducer lightProducer, KafkaConsumerConfig config, String responseBody, int statusCode, int recordSize, List<AuditRecord> auditRecords, boolean dlqLastRetry) {
        if(responseBody != null) {
            long start = System.currentTimeMillis();
            List<Map<String, Object>> results = JsonMapper.string2List(responseBody);
            if (results.size() != recordSize) {
                // if the string2List failed, then a RuntimeException has thrown already.
                // https://github.com/networknt/kafka-sidecar/issues/70 if the response size doesn't match the record size
                logger.error("The response size " + results.size() + " does not match the record size " + recordSize);
                throw new RuntimeException("The response size " + results.size() + " does not match the record size " + recordSize);
            }
			// count failed records
            AtomicInteger failedRecords = new AtomicInteger();

            var threshold = config.getBatchRollbackThreshold() * results.size() / 100.0;

            results.forEach((result)->{
                if(!ObjectUtils.isEmpty(result.get("processed")) && !Boolean.parseBoolean(result.get("processed").toString())){
                    failedRecords.getAndIncrement();
                }
            });

            // if failed records exceeds the threshold, then we will rollback entire batch
            if (failedRecords.get() >= threshold) {
                logger.error("Failed records count {} out of result batch size {} exceeds the failure threshold percentage {} " +
                        "in the batch, will rollback the entire batch",failedRecords,results.size(),config.getBatchRollbackThreshold()   );
                throw new RollbackException("Failed records " + failedRecords + " exceeds the threshold");
            }

            for (int i = 0; i < results.size(); i++) {
                ObjectMapper objectMapper = Config.getInstance().getMapper();
                RecordProcessedResult result = objectMapper.convertValue(results.get(i), RecordProcessedResult.class);
                if (config.getDeadLetterEnabled() && !result.isProcessed() && !dlqLastRetry) {
                    try {
                        logger.info("Sending correlation id ::: "+ result.getCorrelationId() + " traceabilityId ::: "+ result.getTraceabilityId() + " to DLQ topic ::: "+ (result.getRecord().getTopic().contains(config.getDeadLetterTopicExt())? result.getRecord().getTopic() : result.getRecord().getTopic() + config.getDeadLetterTopicExt()));
                        ProduceRequest produceRequest = ProduceRequest.create(null, null, null, null,
                                null, null,null, null,null, null, null );
                        ProduceRecord produceRecord = ProduceRecord.create(null,null, null, null, null);
                        produceRecord.setKey(Optional.of(objectMapper.readTree(objectMapper.writeValueAsString(result.getRecord().getKey()))));
                        produceRecord.setValue(Optional.of(objectMapper.readTree(objectMapper.writeValueAsString(result.getRecord().getValue()))));
                       if(!ObjectUtils.isEmpty(result.getCorrelationId())){
                            produceRecord.setCorrelationId(Optional.ofNullable(result.getCorrelationId()));
                        }
                        else if(!ObjectUtils.isEmpty(result.getRecord().getHeaders().get(Constants.CORRELATION_ID_STRING))){
                            produceRecord.setCorrelationId(Optional.ofNullable(result.getRecord().getHeaders().get(Constants.CORRELATION_ID_STRING).toString()));
                        }

                        if(!ObjectUtils.isEmpty(result.getTraceabilityId())){
                            produceRecord.setTraceabilityId(Optional.ofNullable(result.getTraceabilityId()));
                        }
                        else if(!ObjectUtils.isEmpty(result.getRecord().getHeaders().get(Constants.TRACEABILITY_ID_STRING))){
                            produceRecord.setTraceabilityId(Optional.ofNullable(result.getRecord().getHeaders().get(Constants.TRACEABILITY_ID_STRING).toString()));
                        }
                        produceRecord.setHeaders(Optional.empty());
                        if(!ObjectUtils.isEmpty(result.getRecord().getTimestamp()) && result.getRecord().getTimestamp() >0) {
                            produceRecord.setTimestamp(Optional.of(result.getRecord().getTimestamp()));
                        }
                        produceRequest.setRecords(Arrays.asList(produceRecord));
                        // populate the keyFormat and valueFormat from kafka-producer.yml if request doesn't have them.
                        if(config.getKeyFormat() != null) {
                            produceRequest.setKeyFormat(Optional.of(EmbeddedFormat.valueOf(config.getKeyFormat().toUpperCase())));
                        }
                        if(config.getValueFormat() != null) {
                            produceRequest.setValueFormat(Optional.of(EmbeddedFormat.valueOf(config.getValueFormat().toUpperCase())));
                        }
                        org.apache.kafka.common.header.Headers headers = kafkaConsumerManager.populateHeaders(result);
                        CompletableFuture<ProduceResponse> responseFuture = lightProducer.produceWithSchema(result.getRecord().getTopic().contains(config.getDeadLetterTopicExt())? result.getRecord().getTopic() : result.getRecord().getTopic() + config.getDeadLetterTopicExt(), ServerConfig.getInstance().getServiceId(), Optional.empty(), produceRequest, headers, auditRecords);
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
                        logger.error("Could not process record for traceability id ::: "+ result.getTraceabilityId() + ", correlation id ::: "+ result.getCorrelationId() + " to produce record for DLQ, will skip and proceed for next record ", e);
                        AuditRecord auditRecord = new AuditRecord();
                        auditRecord.setTopic(result.getRecord().getTopic().contains(config.getDeadLetterTopicExt())? result.getRecord().getTopic() : result.getRecord().getTopic() + config.getDeadLetterTopicExt());
                        auditRecord.setAuditType(AuditRecord.AuditType.PRODUCER);
                        auditRecord.setAuditStatus(AuditRecord.AuditStatus.FAILURE);
                        auditRecord.setServiceId(ServerConfig.getInstance().getServiceId());
                        auditRecord.setStacktrace(e.getMessage());
                        auditRecord.setOffset(0);
                        auditRecord.setPartition(0);
                        auditRecord.setTraceabilityId(result.getCorrelationId());
                        auditRecord.setCorrelationId(result.getCorrelationId());
                        auditRecord.setTimestamp(System.currentTimeMillis());
                        auditRecord.setKey(result.getKey());
                        auditRecord.setId(UUID.randomUUID().toString());
                        writeAuditLog(auditRecord, config.getAuditTarget(), config.getAuditTopic());
                    }
                }
                if(config.getAuditEnabled())  reactiveConsumerAuditLog(result, config.getAuditTarget(), config.getAuditTopic());
            }
            if(logger.isDebugEnabled()) {
                logger.debug("Response processing total time is " + (System.currentTimeMillis() - start));
            }
        } else {
            // https://github.com/networknt/kafka-sidecar/issues/70 to check developers errors.
            // throws exception if the body is empty when the response code is successful.
            logger.error("Response body is empty with success status code is " + statusCode);
            throw new RuntimeException("Response Body is empty with success status code " + statusCode);
        }

    }

}
