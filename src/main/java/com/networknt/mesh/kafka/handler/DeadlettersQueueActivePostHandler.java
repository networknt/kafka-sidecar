package com.networknt.mesh.kafka.handler;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.entity.AuditRecord;
import com.networknt.kafka.entity.RecordProcessedResult;
import com.networknt.kafka.producer.CompletableFutures;
import com.networknt.kafka.producer.ProduceResult;
import com.networknt.mesh.kafka.ProducerStartupHook;
import com.networknt.mesh.kafka.SidecarAuditHelper;
import com.networknt.server.Server;
import com.networknt.utility.Constants;
import io.undertow.server.HttpServerExchange;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class DeadlettersQueueActivePostHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(DeadlettersQueueActivePostHandler.class);
    public static KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    private static String PRODUCER_NOT_ENABLED = "ERR12216";


    public DeadlettersQueueActivePostHandler() {
        //TODO
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        long start = System.currentTimeMillis();
        List<Map<String, Object>> map = (List)exchange.getAttachment(BodyHandler.REQUEST_BODY);
        List<RecordProcessedResult> recordProcessedResultList = Config.getInstance().getMapper().convertValue(map, new TypeReference<List<RecordProcessedResult>>(){});

        if(ProducerStartupHook.producer != null) {
            exchange.dispatch();
            CompletableFuture<List<ProduceResult>> responseFutures = doProduce(recordProcessedResultList);
            List<ProduceResult> result = responseFutures.get();
            exchange.getResponseHeaders().put(io.undertow.util.Headers.CONTENT_TYPE, "application/json");
            exchange.setStatusCode(200);
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            exchange.getResponseSender().send(objectMapper.writeValueAsString(result));
            //TODO change to enable below after light-4j issue fix
         //   exchange.getResponseSender().send(JsonMapper.toJson(result));
        } else {
            setExchangeStatus(exchange, PRODUCER_NOT_ENABLED);
        }
    }

    private CompletableFuture<List<ProduceResult>> doProduce(List<RecordProcessedResult> recordProcessedResultList) {

        List<CompletableFuture<ProduceResult>>  completableFutures =  recordProcessedResultList.stream()
                .map(record -> produce(record))
                .collect(Collectors.toList());

          return    CompletableFutures.allAsList(
                        completableFutures.stream().map(future -> future.thenApply(result -> result)).collect(Collectors.toList()));
    }

    public CompletableFuture<ProduceResult> produce(RecordProcessedResult recordProcessedResult) {
        CompletableFuture<ProduceResult> result = new CompletableFuture<>();
        ProducerStartupHook.producer.send(
                new ProducerRecord<>(
                        recordProcessedResult.getRecord().getTopic() + config.getDeadLetterTopicExt(),
                        null,
                        System.currentTimeMillis(),
                        null,
                        JsonMapper.toJson(recordProcessedResult).getBytes(StandardCharsets.UTF_8),
                        null),
                (metadata, exception) -> {
                    if (exception != null) {
                        result.completeExceptionally(exception);
                    } else {
                        if(config.isAuditEnabled()) {
                            writeAuditLog(recordProcessedResult);
                        }
                        result.complete(ProduceResult.fromRecordMetadata(metadata));
                    }
                });
        return result;
    }

    private void writeAuditLog(RecordProcessedResult result) {
        AuditRecord auditRecord = new AuditRecord();
        auditRecord.setId(UUID.randomUUID().toString());
        auditRecord.setServiceId(Server.getServerConfig().getServiceId());
        auditRecord.setAuditType(AuditRecord.AuditType.REACTIVE_CONSUMER);
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
    }
}
