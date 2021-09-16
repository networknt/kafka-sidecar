package com.networknt.mesh.kafka.service;

import com.networknt.kafka.entity.KsqlDbPullQueryRequest;

import java.util.List;
import java.util.Map;

public interface KsqlDBQueryService {
    List<Map<String, Object>> executeQuery(KsqlDbPullQueryRequest request) throws Exception;
}
