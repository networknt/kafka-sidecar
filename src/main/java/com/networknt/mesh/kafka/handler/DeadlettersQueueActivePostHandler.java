package com.networknt.mesh.kafka.handler;


import com.fasterxml.jackson.core.type.TypeReference;
import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.common.config.KafkaConsumerConfig;
import com.networknt.kafka.entity.AuditRecord;
import com.networknt.kafka.entity.RecordProcessedResult;
import com.networknt.kafka.producer.CompletableFutures;
import com.networknt.kafka.producer.ProduceResult;
import com.networknt.mesh.kafka.ProducerStartupHook;
import com.networknt.mesh.kafka.SidecarAuditHelper;
import com.networknt.mesh.kafka.WriteAuditLog;
import com.networknt.server.Server;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.undertow.server.HttpServerExchange;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
public class DeadlettersQueueActivePostHandler extends WriteAuditLog implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(DeadlettersQueueActivePostHandler.class);
    public static KafkaConsumerConfig config = KafkaConsumerConfig.load();
    private static String PRODUCER_NOT_ENABLED = "ERR12216";
    private static String DLQ_ACTIVE_PROCEDURE_ERROR = "ERR30003";

    public DeadlettersQueueActivePostHandler() {
        //TODO
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        List<Map<String, Object>> map = (List)exchange.getAttachment(BodyHandler.REQUEST_BODY);
        List<RecordProcessedResult> recordProcessedResultList = Config.getInstance().getMapper().convertValue(map, new TypeReference<List<RecordProcessedResult>>(){});

        if(ProducerStartupHook.producer != null) {
            exchange.dispatch();
            try {
                CompletableFuture<List<ProduceResult>> responseFutures = doProduce(recordProcessedResultList);
                List<ProduceResult> result = responseFutures.get();
                exchange.getResponseHeaders().put(io.undertow.util.Headers.CONTENT_TYPE, "application/json");
                exchange.setStatusCode(200);
                exchange.getResponseSender().send(JsonMapper.toJson(result));
            } catch (Exception e) {
                logger.error("error happen: " + e);
                Status status = new Status(DLQ_ACTIVE_PROCEDURE_ERROR);
                status.setDescription(e.getMessage());
                setExchangeStatus(exchange, status);
            }

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
                        recordProcessedResult.getKey().getBytes(StandardCharsets.UTF_8),
                        JsonMapper.toJson(recordProcessedResult.getRecord().getValue()).getBytes(StandardCharsets.UTF_8),
                        populateHeaders(recordProcessedResult)),
                (metadata, exception) -> {
                    if (exception != null) {
                        result.completeExceptionally(exception);
                    } else {
                        if(config.getAuditEnabled()) {
                            activeConsumerAuditLog(recordProcessedResult, config.getAuditTarget(), config.getAuditTopic());
                        }
                        result.complete(ProduceResult.fromRecordMetadata(metadata));
                    }
                });
        return result;
    }

    public Headers populateHeaders(RecordProcessedResult recordProcessedResult) {
        Headers headers = new RecordHeaders();
        if (recordProcessedResult.getCorrelationId()!=null) {
            headers.add(Constants.CORRELATION_ID_STRING, recordProcessedResult.getCorrelationId().getBytes(StandardCharsets.UTF_8));
        }
        if (recordProcessedResult.getTraceabilityId()!=null) {
            headers.add(Constants.TRACEABILITY_ID_STRING, recordProcessedResult.getTraceabilityId().getBytes(StandardCharsets.UTF_8));
        }
        if (recordProcessedResult.getStacktrace()!=null) {
            headers.add(Constants.STACK_TRACE, recordProcessedResult.getStacktrace().getBytes(StandardCharsets.UTF_8));
        }
        Map<String, String> recordHeaders = recordProcessedResult.getRecord().getHeaders();
        if (recordHeaders!=null && recordHeaders.size()>0) {
            recordHeaders.keySet().stream().forEach(h->{
                if (recordHeaders.get(h)!=null) {
                    headers.add(h, recordHeaders.get(h).getBytes(StandardCharsets.UTF_8));
                }
            });
        }

        return headers;
    }
}
