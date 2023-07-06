package com.networknt.mesh.kafka.util;

import com.networknt.config.JsonMapper;
import com.networknt.kafka.entity.AuditRecord;
import com.networknt.kafka.entity.util.AuditRecordCreation;
import com.networknt.server.Server;
import com.networknt.utility.ObjectUtils;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class AuditRecordUtil {
    private static final Logger logger= LoggerFactory.getLogger(AuditRecordUtil.class);

    private AuditRecordUtil(){ }

    public static void publishAuditRecord(ProcessorContext processorContext, Record<String, ?> auditRecord, RecordMetadata recordMetadata,
                                          String reasonText, AuditRecord.AuditType auditType, AuditRecord.AuditStatus auditStatus) {

        AuditRecord createdRecord = AuditRecordCreation.createAuditRecord(UUID.randomUUID().toString(),
                Server.getServerConfig().getServiceId(),
                auditType,
                recordMetadata.topic(),
                recordMetadata.partition(),
                recordMetadata.offset(),
                !ObjectUtils.isEmpty(auditRecord.headers().lastHeader("X-Correlation-Id")) ? new String(auditRecord.headers().lastHeader("X-Correlation-Id").value(), StandardCharsets.UTF_8) : null,
                !ObjectUtils.isEmpty(auditRecord.headers().lastHeader("X-Traceability-Id")) ? new String(auditRecord.headers().lastHeader("X-Traceability-Id").value(), StandardCharsets.UTF_8) : null,
                auditRecord.key(),
                auditStatus,
                new RuntimeException(reasonText).getMessage(),
                System.currentTimeMillis()
        );
        try {
            processorContext.forward(new Record(auditRecord.key(), JsonMapper.objectMapper.writeValueAsString(createdRecord), System.currentTimeMillis()), "AuditSink");
        } catch (Exception e) {
            logger.error("Exception while forwarding audit record ", e);
        }


    }
}
