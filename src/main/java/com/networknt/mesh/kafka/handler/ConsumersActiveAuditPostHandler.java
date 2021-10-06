package com.networknt.mesh.kafka.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.entity.AuditRecord;
import com.networknt.kafka.entity.RecordProcessedResult;
import com.networknt.mesh.kafka.ProducerStartupHook;
import com.networknt.mesh.kafka.SidecarAuditHelper;
import com.networknt.server.Server;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.undertow.server.HttpServerExchange;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class ConsumersActiveAuditPostHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConsumersActiveAuditPostHandler.class);
    public static KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    private static String PRODUCER_NOT_ENABLED = "ERR12216";
    private static String CONSUMER_ACTIVE_AUDIT_ERROR = "ERR30004";

    public ConsumersActiveAuditPostHandler() {
        //TODO
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {

        List<Map<String, Object>> map = (List)exchange.getAttachment(BodyHandler.REQUEST_BODY);
        List<RecordProcessedResult> recordProcessedResultList = Config.getInstance().getMapper().convertValue(map, new TypeReference<>(){});

        if(ProducerStartupHook.producer != null) {
            exchange.dispatch();
            try {
                List<AuditRecord> auditRecords = recordProcessedResultList.stream().map(r->writeAuditLog(r)).collect(Collectors.toList());
                exchange.getResponseHeaders().put(io.undertow.util.Headers.CONTENT_TYPE, "application/json");
                exchange.setStatusCode(200);
                exchange.getResponseSender().send(JsonMapper.toJson(auditRecords));
            } catch (Exception e) {
                logger.error("error happen: " + e);
                Status status = new Status(CONSUMER_ACTIVE_AUDIT_ERROR);
                status.setDescription(e.getMessage());
                setExchangeStatus(exchange, status);
            }

        } else {
            setExchangeStatus(exchange, PRODUCER_NOT_ENABLED);
        }
    }

    private AuditRecord writeAuditLog(RecordProcessedResult result) {
        AuditRecord auditRecord = new AuditRecord();
        auditRecord.setId(UUID.randomUUID().toString());
        auditRecord.setServiceId(Server.getServerConfig().getServiceId());
        auditRecord.setAuditType(AuditRecord.AuditType.ACTIVE_CONSUMER);
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
        auditRecord.setAuditStatus(result.isProcessed() ? AuditRecord.AuditStatus.SUCCESS : AuditRecord.AuditStatus.FAILURE);
        if(KafkaConsumerConfig.AUDIT_TARGET_TOPIC.equals(config.getAuditTarget())) {
            ProducerStartupHook.producer.send(
                    new ProducerRecord<>(
                            config.getAuditTopic(),
                            null,
                            System.currentTimeMillis(),
                            auditRecord.getCorrelationId() != null ? auditRecord.getCorrelationId().getBytes(StandardCharsets.UTF_8) : auditRecord.getKey(),
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
        return auditRecord;
    }
}
