package com.networknt.mesh.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.kafka.entity.AuditRecord;
import com.networknt.utility.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.function.Consumer;

/**
 * A helper class to write audit log to a file sidecar.log into filesystem instead of
 * a kafka topic. This class is used only when auditTarget is set to `logfile` in the
 * kafka-consumer.yml and kafka-producer.yml configuration files.
 *
 * @author Steve Hu
 */
public class SidecarAuditHelper {
    public static String SIDECAR_LOGGER = "Sidecar";
    private static Logger logger = LoggerFactory.getLogger(SidecarAuditHelper.class);

    /**
     * A help method to log result pojo
     * @param auditRecord the object contains info that needs to be logged
     */
    public static void logResult(AuditRecord auditRecord) {
        Consumer<String> loggerFunc = getLoggerFuncBasedOnLevel();
        logResultUsingJson(auditRecord, loggerFunc);
    }

    /**
     *
     * @param auditRecord an AuditRecord object contains auditing info which needs to be logged.
     * @param loggerFunc Consumer<T> getLoggerFuncBasedOnLevel()
     */
    private static void logResultUsingJson(AuditRecord auditRecord, Consumer<String> loggerFunc) {
        ObjectMapper mapper = new ObjectMapper();
        String resultJson = "";
        try {
            resultJson = mapper.writeValueAsString(auditRecord);
        } catch (JsonProcessingException e) {
            logger.error(e.toString());
        }
        if(StringUtils.isNotBlank(resultJson)){
            loggerFunc.accept(resultJson);
        }
    }

    /**
     * @return Consumer<String> of logger info level.
     */
    private static Consumer<String> getLoggerFuncBasedOnLevel() {
        return LoggerFactory.getLogger(SIDECAR_LOGGER)::info;
    }
}
