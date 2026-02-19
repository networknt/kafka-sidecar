package com.networknt.mesh.kafka;

import com.networknt.kafka.consumer.exception.RollbackException;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteAuditLogTest {

    private final WriteAuditLog writeAuditLog = new WriteAuditLog();

    private Map<String, Object> record(boolean processed) {
        Map<String, Object> result = new HashMap<>();
        result.put("processed", processed);
        return result;
    }

    /**
     * Rollback occurs when the number of failed records exceeds the threshold.
     * 4 failures out of 5 records with threshold=50%: 4 >= 2.5 -> rollback
     */
    @Test(expected = RollbackException.class)
    public void testRollbackWhenFailuresExceedThreshold() {
        List<Map<String, Object>> results = Arrays.asList(
                record(false), record(false), record(false), record(false), record(true));
        writeAuditLog.checkBatchRollbackThreshold(results, 50);
    }

    /**
     * No rollback when the number of failed records is below the threshold.
     * 1 failure out of 5 records with threshold=50%: 1 < 2.5 -> no rollback
     */
    @Test
    public void testNoRollbackWhenFailuresBelowThreshold() {
        List<Map<String, Object>> results = Arrays.asList(
                record(false), record(true), record(true), record(true), record(true));
        writeAuditLog.checkBatchRollbackThreshold(results, 50);
    }

    /**
     * Rollback occurs when the number of failed records exactly equals the threshold.
     * 2 failures out of 4 records with threshold=50%: threshold=2.0, 2 >= 2.0 -> rollback
     */
    @Test(expected = RollbackException.class)
    public void testRollbackWhenFailuresEqualThreshold() {
        List<Map<String, Object>> results = Arrays.asList(
                record(false), record(false), record(true), record(true));
        writeAuditLog.checkBatchRollbackThreshold(results, 50);
    }

    /**
     * Rollback occurs for a small batch where the single record fails.
     * 1 failure out of 1 record with threshold=50%: threshold=0.5, 1 >= 0.5 -> rollback
     */
    @Test(expected = RollbackException.class)
    public void testSmallBatchRollbackWhenAllFail() {
        List<Map<String, Object>> results = Arrays.asList(record(false));
        writeAuditLog.checkBatchRollbackThreshold(results, 50);
    }

    /**
     * No rollback for a small batch where no records fail.
     * 0 failures out of 1 record with threshold=50%: threshold=0.5, 0 < 0.5 -> no rollback
     */
    @Test
    public void testSmallBatchNoRollbackWhenNoneFail() {
        List<Map<String, Object>> results = Arrays.asList(record(true));
        writeAuditLog.checkBatchRollbackThreshold(results, 50);
    }
}
