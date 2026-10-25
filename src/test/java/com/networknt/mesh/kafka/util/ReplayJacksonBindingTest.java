package com.networknt.mesh.kafka.util;

import com.networknt.config.Config;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.consumer.KafkaConsumerManager;
import com.networknt.kafka.entity.*;
import com.networknt.kafka.producer.SidecarProducer;
import com.networknt.mesh.kafka.WriteAuditLog;
import com.networknt.utility.Constants;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@Timeout(15)
class ReplayJacksonBindingTest {
    private String previousKeyFormat;
    private String previousValueFormat;
    private boolean previousFirstBatch;

    @BeforeEach
    void setFormats() {
        var config = ActiveConsumerStreamsAppMessageHandle.consumerConfig;
        previousKeyFormat = config.getKeyFormat();
        previousValueFormat = config.getValueFormat();
        previousFirstBatch = ActiveConsumerStreamsAppMessageHandle.firstBatch;
        config.setKeyFormat("json");
        config.setValueFormat("json");
        ActiveConsumerStreamsAppMessageHandle.firstBatch = true;
    }

    @AfterEach
    void restoreFormats() {
        ActiveConsumerStreamsAppMessageHandle.consumerConfig.setKeyFormat(previousKeyFormat);
        ActiveConsumerStreamsAppMessageHandle.consumerConfig.setValueFormat(previousValueFormat);
        ActiveConsumerStreamsAppMessageHandle.firstBatch = previousFirstBatch;
    }

    @Test
    void streamedReplayBindsConsumerRecordsAndRebuildsProducerTrees() {
        var producer = mock(SidecarProducer.class);
        var manager = mock(KafkaConsumerManager.class);
        // Hold completion so this test isolates binding/forwarding from async audit publication.
        when(producer.produceWithSchema(anyString(), any(), any(), any(), any(), anyList()))
                .thenReturn(new CompletableFuture<ProduceResponse>());
        when(manager.commitOffsets(anyString(), anyString(), eq(false), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        TopicReplayMetadata metadata = new TopicReplayMetadata();
        metadata.setTopicName("events");
        metadata.setDestinationTopic("replayed");
        metadata.setConsumerGroup("replay-group");
        metadata.setStartOffset(5);
        metadata.setEndOffset(8);
        String input = Config.getInstance().getMapper().writeValueAsString(
                List.of(consumerRecord(5), consumerRecord(6)));

        long next = new ActiveConsumerStreamsAppMessageHandle().listenOnMessage(
                producer, input, 5, metadata, manager, "instance");

        assertEquals(7, next);
        var request = ArgumentCaptor.forClass(ProduceRequest.class);
        verify(producer).produceWithSchema(eq("replayed"), any(), any(), request.capture(), any(), anyList());
        assertEquals(2, request.getValue().getRecords().size());
        for (ProduceRecord record : request.getValue().getRecords()) assertTrees(record);
        var commit = ArgumentCaptor.forClass(ConsumerOffsetCommitRequest.class);
        verify(manager).commitOffsets(eq("replay-group"), eq("instance"), eq(false), commit.capture(), any());
        assertEquals(6, commit.getValue().getOffsets().getFirst().getOffset());
    }

    @Test
    void malformedReplayFailsBeforeProducingOrCommitting() {
        var producer = mock(SidecarProducer.class);
        var manager = mock(KafkaConsumerManager.class);
        RuntimeException error = assertThrows(RuntimeException.class,
                () -> new ActiveConsumerStreamsAppMessageHandle().listenOnMessage(
                        producer, "[broken", 5, new TopicReplayMetadata(), manager, "instance"));
        assertTrue(error.getMessage().startsWith("ActiveConsumerStreamsAppMessageHandle exception while processing read message"));
        verifyNoInteractions(producer, manager);
    }

    @Test
    void auditResponseBindsFailedRecordAndRebuildsDlqTrees() {
        var producer = mock(SidecarProducer.class);
        var manager = mock(KafkaConsumerManager.class);
        when(manager.populateHeaders(any())).thenReturn(new RecordHeaders());
        when(producer.produceWithSchema(anyString(), any(), any(), any(), any(), anyList()))
                .thenReturn(new CompletableFuture<ProduceResponse>());
        var audits = new CapturingAudits();
        String response = Config.getInstance().getMapper().writeValueAsString(List.of(
                Map.of("processed", false, "record", consumerRecord(5)),
                Map.of("processed", true, "record", consumerRecord(6))));

        audits.processResponse(manager, producer, config(), response, 200, 2, new ArrayList<>(), false);

        var request = ArgumentCaptor.forClass(ProduceRequest.class);
        verify(producer).produceWithSchema(eq("events.dlq"), any(), any(), request.capture(), any(), anyList());
        ProduceRecord record = request.getValue().getRecords().getFirst();
        assertTrees(record);
        assertEquals(123L, record.getTimestamp().orElseThrow());
        assertTrue(record.getHeaders().isEmpty());
        assertTrue(audits.failures.isEmpty(), "Do not silently route binding failures to audit logging");
    }

    @Test
    void malformedAuditRecordFailsBeforeProducing() {
        var producer = mock(SidecarProducer.class);
        var manager = mock(KafkaConsumerManager.class);
        assertThrows(RuntimeException.class, () -> new CapturingAudits().processResponse(
                manager, producer, config(), "[{\"processed\":true,\"record\":\"not a record\"}]",
                200, 1, new ArrayList<>(), false));
        verifyNoInteractions(producer, manager);
    }

    private static KafkaConsumerConfig config() {
        var config = new KafkaConsumerConfig();
        config.setDeadLetterEnabled(true);
        config.setDeadLetterTopicExt(".dlq");
        config.setBatchRollbackThreshold(100);
        config.setAuditEnabled(false);
        config.setKeyFormat("json");
        config.setValueFormat("json");
        return config;
    }

    private static Map<String, Object> consumerRecord(long offset) {
        return Map.of("topic", "events", "partition", 0, "offset", offset, "timestamp", 123,
                "key", Map.of("id", 17), "value", Map.of("array", Arrays.asList(null, true, 1.25, "雪")),
                "headers", Map.of(Constants.CORRELATION_ID_STRING, "corr",
                        Constants.TRACEABILITY_ID_STRING, "trace"));
    }

    private static void assertTrees(ProduceRecord record) {
        assertEquals(17, record.getKey().orElseThrow().get("id").asInt());
        var array = record.getValue().orElseThrow().get("array");
        assertTrue(array.get(0).isNull());
        assertTrue(array.get(1).booleanValue());
        assertEquals(1.25, array.get(2).asDouble());
        assertEquals("雪", array.get(3).asString());
        assertEquals("corr", record.getCorrelationId().orElseThrow());
        assertEquals("trace", record.getTraceabilityId().orElseThrow());
    }

    private static final class CapturingAudits extends WriteAuditLog {
        final List<AuditRecord> failures = new ArrayList<>();
        @Override
        protected void writeAuditLog(AuditRecord record, String target, String topic) {
            failures.add(record);
        }
    }
}
