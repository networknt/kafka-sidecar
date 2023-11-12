package com.networknt.mesh.kafka.handler;

import com.networknt.mesh.kafka.ProducerStartupHook;
import com.networknt.mesh.kafka.WriteAuditLog;
import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.handler.LightHttpHandler;
import com.networknt.httpstring.AttachmentConstants;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.common.KafkaProducerConfig;
import com.networknt.kafka.common.KafkaStreamsConfig;
import com.networknt.kafka.entity.*;
import com.networknt.kafka.producer.KafkaHeadersCarrier;
import com.networknt.kafka.producer.NativeLightProducer;
import com.networknt.kafka.producer.SidecarProducer;
import com.networknt.server.Server;
import com.networknt.server.ServerConfig;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.utility.Constants;
import com.networknt.utility.StringUtils;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.undertow.server.HttpServerExchange;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * This class is responsible for triggering replay messages into replay controller topic
 */
public class TopicReplayPostHandler extends WriteAuditLog implements LightHttpHandler  {

    private static final Logger logger = LoggerFactory.getLogger(TopicReplayPostHandler.class);
    public static KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    public static KafkaStreamsConfig streamsConfig = (KafkaStreamsConfig) Config.getInstance().getJsonObjectConfig(KafkaStreamsConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    public static KafkaProducerConfig producerConfig = (KafkaProducerConfig) Config.getInstance().getJsonObjectConfig(KafkaProducerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    private static String STATUS_ACCEPTED = "SUC10202";
    private static String PRODUCER_NOT_ENABLED = "ERR12216";
    private static String TOPICREPLAY_METADATA_ERROR = "ERR12218";
    private static String RUNTIME_EXCEPTION = "ERR10010";
    SidecarProducer lightProducer;
    private String callerId = "unknown";
    public List<AuditRecord> auditRecords = new ArrayList<>();

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        {
            if(ProducerStartupHook.producer != null) {
                exchange.dispatch();
                try{
                long start = System.currentTimeMillis();
                lightProducer = (SidecarProducer) SingletonServiceFactory.getBean(NativeLightProducer.class);
                Map<String, Object> map = (Map) exchange.getAttachment(BodyHandler.REQUEST_BODY);
                TopicReplayMetadata topicReplayMetadata = Config.getInstance().getMapper().convertValue(map, TopicReplayMetadata.class);
                if(StringUtils.isEmpty(topicReplayMetadata.getTopicName())){
                    setExchangeStatus(exchange, TOPICREPLAY_METADATA_ERROR, "topic name can not be empty");
                    return;
                }else if(!config.getTopic().contains(topicReplayMetadata.getTopicName())) {
                    setExchangeStatus(exchange, TOPICREPLAY_METADATA_ERROR, "provided topic in request must match one of the topics in consuming topic list");
                    return;
                }else if(topicReplayMetadata.getPartition() < 0) {
                    setExchangeStatus(exchange, TOPICREPLAY_METADATA_ERROR, "partition can not be negative");
                    return;
                }else if(topicReplayMetadata.getStartOffset() < 0){
                    setExchangeStatus(exchange, TOPICREPLAY_METADATA_ERROR, "replay start offset can not be negative");
                    return;
                }else if(topicReplayMetadata.getEndOffset() < 0){
                    setExchangeStatus(exchange, TOPICREPLAY_METADATA_ERROR, "replay end offset can not be negative");
                    return;
                }else if(topicReplayMetadata.getStartOffset() > topicReplayMetadata.getEndOffset() ){
                    setExchangeStatus(exchange, TOPICREPLAY_METADATA_ERROR, "replay end offset can not be less than start offset");
                    return;
                }else if(topicReplayMetadata.getStartOffset() == topicReplayMetadata.getEndOffset() ){
                    setExchangeStatus(exchange, TOPICREPLAY_METADATA_ERROR, "replay end offset can not be equal to start offset");
                    return;
                }else if(StringUtils.isEmpty(topicReplayMetadata.getConsumerGroup())){
                    setExchangeStatus(exchange, TOPICREPLAY_METADATA_ERROR, "replay consumer group can not be empty");
                    return;
                }
                else if(topicReplayMetadata.isStreamingApp() && StringUtils.isEmpty(topicReplayMetadata.getDestinationTopic()) ){
                    setExchangeStatus(exchange, TOPICREPLAY_METADATA_ERROR, "Destination topic can not be empty for a streaming app");
                    return;
                }else{
                    /**
                     *
                     * We can add a condition for consumer group to contain dlq keyword incase of DLQ replay ?
                     */

                    String topic=  topicReplayMetadata.getTopicName();
                    if(topicReplayMetadata.isDlqIndicator()){
                        topic= topicReplayMetadata.getTopicName().concat(config.getDeadLetterTopicExt());
                        topicReplayMetadata.setTopicName(topic);
                    }
                    if (logger.isInfoEnabled()) {
                        logger.info("TopicReplayPostHandler handleRequest send replay metadata for topic {}" , topic);
                    }

                    /**
                     * Create produce record with only key and value
                     */
                    ProduceRecord produceRecord= new ProduceRecord(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),Optional.empty());
                    produceRecord.setKey(Optional.of(JsonMapper.objectMapper.readTree(JsonMapper.objectMapper.writeValueAsString(topic))));
                    produceRecord.setValue(Optional.of(JsonMapper.objectMapper.readTree(JsonMapper.objectMapper.writeValueAsString(topicReplayMetadata))));
                    produceRecord.setTraceabilityId(Optional.of(UUID.randomUUID().toString()));
                    produceRecord.setCorrelationId(Optional.of(UUID.randomUUID().toString()));
                    /**
                     * Create produce request to set produce record
                     * keyFormat is by default set to String
                     * valueFormat is by default set to jsonschema
                     */
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
                    produceRequest.setRecords(Arrays.asList(produceRecord));
                    produceRequest.setValueFormat(Optional.of(EmbeddedFormat.JSONSCHEMA));
                    produceRequest.setKeyFormat(Optional.of(EmbeddedFormat.STRING));

                    /**
                     * After producing into controller topic, we will write audit for it.
                     */
                    Headers headers = populateHeaders(exchange, producerConfig, topic);
                    CompletableFuture<ProduceResponse> responseFuture = lightProducer.produceWithSchema(streamsConfig.getDeadLetterControllerTopic(), ServerConfig.getInstance().getServiceId(), Optional.empty(), produceRequest, headers, auditRecords);
                    responseFuture.whenCompleteAsync((response, throwable) -> {
                        // write the audit log here.
                        synchronized (auditRecords) {
                            if (auditRecords != null && auditRecords.size() > 0) {
                                auditRecords.forEach(ar -> {
                                    writeAuditLog(ar, config.getAuditTarget(), config.getAuditTopic());
                                });
                                // clean up the audit entries
                                auditRecords.clear();
                            }
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug("TopicReplayPostHandler handleRequest total time is {}" , (System.currentTimeMillis() - start));
                            logger.debug("Message produced to offset {}", response.getOffsets().get(0) );

                        }
                        exchange.getResponseHeaders().put(io.undertow.util.Headers.CONTENT_TYPE, "application/json");
                        exchange.getResponseSender().send(JsonMapper.toJson(response));
                    });
                }
            } catch(Exception e){
                    setExchangeStatus(exchange, RUNTIME_EXCEPTION );
                    logger.error("Error while producing message to replay topic ", e);
                    return;
                }
            }
            else {
                setExchangeStatus(exchange, PRODUCER_NOT_ENABLED);
                return;
            }
        }

    }

    public Headers populateHeaders(HttpServerExchange exchange, KafkaProducerConfig config, String topic) {
        Headers headers = new RecordHeaders();
        String token = exchange.getRequestHeaders().getFirst(Constants.AUTHORIZATION_STRING);
        if(token != null) {
            headers.add(Constants.AUTHORIZATION_STRING, token.getBytes(StandardCharsets.UTF_8));
        }
        if(config.isInjectOpenTracing()) {
            // maybe we should move this to the ProduceRecord in the future like the correlationId and traceabilityId.
            Tracer tracer = exchange.getAttachment(AttachmentConstants.EXCHANGE_TRACER);
            if(tracer != null && tracer.activeSpan() != null) {
                Tags.SPAN_KIND.set(tracer.activeSpan(), Tags.SPAN_KIND_PRODUCER);
                Tags.MESSAGE_BUS_DESTINATION.set(tracer.activeSpan(), topic);
                tracer.inject(tracer.activeSpan().context(), Format.Builtin.TEXT_MAP, new KafkaHeadersCarrier(headers));
            }
        }

        // remove the correlationId from the HTTP header and moved it to the ProduceRecord as it is per record attribute.
        // remove the traceabilityId from the HTTP header and moved it to the ProduceRecord as it is per record attribute.

        if(config.isInjectCallerId()) {
            headers.add(Constants.CALLER_ID_STRING, callerId.getBytes(StandardCharsets.UTF_8));
        }
        return headers;
    }
}
