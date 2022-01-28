package com.networknt.mesh.kafka;

import com.google.protobuf.ByteString;
import com.networknt.config.JsonMapper;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.entity.AuditRecord;
import com.networknt.kafka.entity.RecordProcessedResult;
import com.networknt.server.Server;
import com.networknt.utility.Constants;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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
        auditRecord.setServiceId(Server.getServerConfig().getServiceId());
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

    protected AuditRecord auditFromRecordMetadata(RecordMetadata rmd, Exception e, Headers headers, Optional<ByteString> key, Optional<String> traceabilityId, boolean produced) {
        AuditRecord auditRecord = new AuditRecord();
        auditRecord.setId(UUID.randomUUID().toString());
        auditRecord.setServiceId(Server.getServerConfig().getServiceId());
        auditRecord.setAuditType(AuditRecord.AuditType.PRODUCER);
        if(rmd != null) {
            auditRecord.setTopic(rmd.topic());
            auditRecord.setPartition(rmd.partition());
            auditRecord.setOffset(rmd.offset());
        } else {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            auditRecord.setStacktrace(sw.toString());
        }
        Header cHeader = headers.lastHeader(Constants.CORRELATION_ID_STRING);
        if(cHeader != null) {
            auditRecord.setCorrelationId(new String(cHeader.value(), StandardCharsets.UTF_8));
        }

        if(traceabilityId.isPresent()) {
            auditRecord.setTraceabilityId(traceabilityId.get());
        }

        auditRecord.setAuditStatus(produced ? AuditRecord.AuditStatus.SUCCESS : AuditRecord.AuditStatus.FAILURE);
        auditRecord.setTimestamp(System.currentTimeMillis());
        if(key.isPresent()) auditRecord.setKey(key.get().toString(StandardCharsets.UTF_8));
        return auditRecord;
    }

    protected void writeAuditLog(AuditRecord auditRecord, String auditTarget, String auditTopic) {
        if(KafkaConsumerConfig.AUDIT_TARGET_TOPIC.equals(auditTarget)) {
            ProducerStartupHook.producer.send(
                    new ProducerRecord<>(
                            auditTopic,
                            null,
                            System.currentTimeMillis(),
                            auditRecord.getTraceabilityId() != null ? auditRecord.getTraceabilityId().getBytes(StandardCharsets.UTF_8) : auditRecord.getKey().getBytes(StandardCharsets.UTF_8),
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
