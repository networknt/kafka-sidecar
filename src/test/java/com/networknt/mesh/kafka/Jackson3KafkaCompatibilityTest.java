package com.networknt.mesh.kafka;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.networknt.config.Config;
import com.networknt.kafka.entity.AuditRecord;
import com.networknt.kafka.entity.ProduceRequest;
import com.networknt.mesh.kafka.util.ConvertToList;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class Jackson3KafkaCompatibilityTest {
    @Test
    void restProduceBodiesContainJackson3TreesAndPreserveMetadata() {
        ProduceRequest request = Config.getInstance().getMapper().readValue("""
                {"keyFormat":"JSON","valueFormat":"JSON","records":[{
                "key":{"id":1},"value":{"array":[null,1.25,"雪"]},
                "headers":{"x":"y"},"timestamp":123}]}
                """, ProduceRequest.class);
        var record = request.getRecords().getFirst();
        assertEquals(1, record.getKey().orElseThrow().get("id").asInt());
        var array = record.getValue().orElseThrow().get("array");
        assertTrue(array.get(0).isNull());
        assertEquals(1.25, array.get(1).asDouble());
        assertEquals("雪", array.get(2).asString());
        assertEquals(Map.of("x", "y"), record.getHeaders().orElseThrow());
        assertEquals(123L, record.getTimestamp().orElseThrow());
    }

    @Test
    void listReaderRetainsMapValuesAndWrapsMalformedJson() {
        var mapper = Config.getInstance().getMapper();
        var list = ConvertToList.string2List(mapper, "[{\"processed\":true,\"offset\":12}]");
        assertEquals(Boolean.TRUE, list.getFirst().get("processed"));
        assertEquals(12, list.getFirst().get("offset"));
        RuntimeException error = assertThrows(RuntimeException.class,
                () -> ConvertToList.string2List(mapper, "[broken"));
        assertEquals(RuntimeException.class, error.getClass());
        assertInstanceOf(JacksonException.class, error.getCause());
    }

    @Test
    void logfileAuditRemainsJsonWithTheExistingFieldNames() {
        AuditRecord record = new AuditRecord();
        record.setId("fixed-id");
        record.setTopic("events");
        record.setAuditType(AuditRecord.AuditType.ACTIVE_CONSUMER);
        record.setAuditStatus(AuditRecord.AuditStatus.SUCCESS);
        record.setTimestamp(123L);
        Logger logger = (Logger) LoggerFactory.getLogger(SidecarAuditHelper.SIDECAR_LOGGER);
        Level previousLevel = logger.getLevel();
        logger.setLevel(Level.INFO);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            SidecarAuditHelper.logResult(record);
            assertEquals(1, appender.list.size());
            var json = Config.getInstance().getMapper().readTree(appender.list.getFirst().getFormattedMessage());
            assertEquals("fixed-id", json.get("id").asString());
            assertEquals("events", json.get("topic").asString());
            assertEquals("ACTIVE_CONSUMER", json.get("auditType").asString());
            assertEquals("SUCCESS", json.get("auditStatus").asString());
            assertEquals(123L, json.get("timestamp").asLong());
        } finally {
            logger.detachAppender(appender);
            appender.stop();
            logger.setLevel(previousLevel);
        }
    }
}
