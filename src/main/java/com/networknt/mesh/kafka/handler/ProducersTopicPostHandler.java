package com.networknt.mesh.kafka.handler;

import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.common.KafkaProducerConfig;
import com.networknt.kafka.entity.*;
import com.networknt.kafka.producer.*;
import com.networknt.mesh.kafka.ProducerStartupHook;
import com.networknt.mesh.kafka.WriteAuditLog;
import com.networknt.server.ServerConfig;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.utility.Constants;
import com.networknt.kafka.entity.EmbeddedFormat;
import io.undertow.server.HttpServerExchange;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * The producer endpoint that can receive request from the backend service messages and push them
 * into a kafka topic as path parameter in a transaction. Only when the message is successfully
 * acknowledged from Kafka, the response will be sent to the caller. This will guarantee that no
 * message will be missed in the process. However, due to the network issue, sometimes, the ack
 * might not receive by the caller after the messages are persisted. So duplicated message might
 * be received, the handle will have a queue to cache the last several messages to remove duplicated
 * message possible.
 *
 * This handler will only work when the ProducerStartupHook is enabled in the service.yml file. If
 * the startup hook is not called, an error message will be returned when this endpoint is called.
 *
 * @author Steve Hu
 */
public class ProducersTopicPostHandler extends WriteAuditLog implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ProducersTopicPostHandler.class);
    private static String STATUS_ACCEPTED = "SUC10202";
    private static String PRODUCER_NOT_ENABLED = "ERR12216";

    private String callerId = "unknown";
    SidecarProducer lightProducer;
    private KafkaProducerConfig config;
    public List<AuditRecord> auditRecords = new ArrayList<>();

    public ProducersTopicPostHandler() {
        // constructed this handler only if the startup hook producer is not empty.
        if(ProducerStartupHook.producer != null) {
            lightProducer = (SidecarProducer) SingletonServiceFactory.getBean(NativeLightProducer.class);
            config = lightProducer.config;
            if (config.isInjectCallerId()) {
                ServerConfig serverConfig = ServerConfig.getInstance();
                if (serverConfig != null) {
                    callerId = serverConfig.getServiceId();
                }
            }
        }
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if(ProducerStartupHook.producer != null) {
            // the topic is the path parameter, so it is required and cannot be null.
            String topic = exchange.getQueryParameters().get("topic").getFirst();
            long start = System.currentTimeMillis();
            if (logger.isInfoEnabled()) {
                logger.info("ProducerTopicPostHandler handleRequest start with topic " + topic);
            }
            exchange.dispatch();
            Map<String, Object> map = (Map) exchange.getAttachment(BodyHandler.REQUEST_BODY);
            ProduceRequest produceRequest = Config.getInstance().getMapper().convertValue(map, ProduceRequest.class);
            // populate the keyFormat and valueFormat from kafka-producer.yml if request doesn't have them.
            if(produceRequest.getKeyFormat().isEmpty() && config.getKeyFormat() != null) {
                produceRequest.setKeyFormat(Optional.of(EmbeddedFormat.valueOf(config.getKeyFormat().toUpperCase())));
            }
            if(produceRequest.getValueFormat().isEmpty() && config.getValueFormat() != null) {
                produceRequest.setValueFormat(Optional.of(EmbeddedFormat.valueOf(config.getValueFormat().toUpperCase())));
            }
            Headers headers = populateHeaders(exchange, config, topic);
            CompletableFuture<ProduceResponse> responseFuture = lightProducer.produceWithSchema(topic, ServerConfig.getInstance().getServiceId(), Optional.empty(), produceRequest, headers, auditRecords);
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
                    logger.debug("ProducerTopicPostHandler handleRequest total time is " + (System.currentTimeMillis() - start));
                }
                exchange.getResponseHeaders().put(io.undertow.util.Headers.CONTENT_TYPE, "application/json");
                exchange.getResponseSender().send(JsonMapper.toJson(response));
            });
        } else {
            setExchangeStatus(exchange, PRODUCER_NOT_ENABLED);
        }
    }

    public Headers populateHeaders(HttpServerExchange exchange, KafkaProducerConfig config, String topic) {
        Headers headers = new RecordHeaders();
        String token = exchange.getRequestHeaders().getFirst(Constants.AUTHORIZATION_STRING);
        if(token != null) {
            headers.add(Constants.AUTHORIZATION_STRING, token.getBytes(StandardCharsets.UTF_8));
        }
        /*
        if(config.isInjectOpenTracing()) {
            // maybe we should move this to the ProduceRecord in the future like the correlationId and traceabilityId.
            Tracer tracer = exchange.getAttachment(AttachmentConstants.EXCHANGE_TRACER);
            if(tracer != null && tracer.activeSpan() != null) {
                Tags.SPAN_KIND.set(tracer.activeSpan(), Tags.SPAN_KIND_PRODUCER);
                Tags.MESSAGE_BUS_DESTINATION.set(tracer.activeSpan(), topic);
                tracer.inject(tracer.activeSpan().context(), Format.Builtin.TEXT_MAP, new KafkaHeadersCarrier(headers));
            }
        }
        */
        // remove the correlationId from the HTTP header and moved it to the ProduceRecord as it is per record attribute.
        // remove the traceabilityId from the HTTP header and moved it to the ProduceRecord as it is per record attribute.

        if(config.isInjectCallerId()) {
            headers.add(Constants.CALLER_ID_STRING, callerId.getBytes(StandardCharsets.UTF_8));
        }
        return headers;
    }

}
