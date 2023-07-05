package com.networknt.mesh.kafka.service;


import com.networknt.client.ClientConfig;
import com.networknt.kafka.entity.KsqlDbPullQueryRequest;
import com.networknt.mesh.kafka.KsqldbActiveConsumerStartupHook;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.exception.KsqlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class KsqlDBQueryServiceImpl implements KsqlDBQueryService{
    private static Logger logger = LoggerFactory.getLogger(KsqlDBQueryService.class);

    @Override
    public List<Map<String, Object>> executeQuery(KsqlDbPullQueryRequest request) {
        BatchedQueryResult batchedQueryResult = null;
        if (request.getProperties().size()>0) {
            batchedQueryResult = KsqldbActiveConsumerStartupHook.client.executeQuery(request.getQuery(), request.getProperties());
        } else {
            batchedQueryResult = KsqldbActiveConsumerStartupHook.client.executeQuery(request.getQuery());
        }
        List<Map<String, Object>> records = new ArrayList<>();
        Collection<CompletableFuture<?>> completableFutures = new HashSet<>();
        if (KsqlDbPullQueryRequest.QueryType.PUSH.equals(request.getQueryType())) {
            completableFutures.add(batchedQueryResult.queryID());
            completableFutures.add(batchedQueryResult.queryID().thenAccept(id->KsqldbActiveConsumerStartupHook.client.terminatePushQuery(id)));
        }
        completableFutures.add(batchedQueryResult);
        completableFutures.add(processResponse(records, batchedQueryResult));
        try {
            CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0])).get(ClientConfig.get().getTimeout(), TimeUnit.MILLISECONDS);
        } catch(Exception e) {
            logger.error("Error happen when the ksql processing.", e);
            throw new KsqlException("Ksql execution error:" + e);
        }

        return records;
    }

    private CompletableFuture<Void> processResponse(List<Map<String, Object>> records,  BatchedQueryResult batchedQueryResult ) {
        return batchedQueryResult.thenAccept(result-> {
            for (Row row: result) {
                Map<String, Object> record = new HashMap<>();
                row.columnNames().stream().forEach(c-> record.put(c, row.getValue(c)));
                records.add(record);
            }
        });

    }
}
