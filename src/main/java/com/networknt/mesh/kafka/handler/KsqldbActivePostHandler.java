package com.networknt.mesh.kafka.handler;


import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.entity.KsqlDbPullQueryRequest;
import com.networknt.mesh.kafka.service.KsqlDBQueryService;
import com.networknt.mesh.kafka.service.KsqlDBQueryServiceImpl;
import com.networknt.status.Status;
import io.undertow.server.HttpServerExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


public class KsqldbActivePostHandler implements LightHttpHandler {
    private static Logger logger = LoggerFactory.getLogger(KsqldbActivePostHandler.class);
    private static String INVALID_KSQL_QUERY = "ERR30002";

    KsqlDBQueryService service;

    public KsqldbActivePostHandler() {
        this.service = new KsqlDBQueryServiceImpl();
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {

        Map<String, Object> map = (Map)exchange.getAttachment(BodyHandler.REQUEST_BODY);
        KsqlDbPullQueryRequest request = Config.getInstance().getMapper().convertValue(map, KsqlDbPullQueryRequest.class);
        logger.info("ksqldb pull query to run:" +    request.getQuery().replace("\\'", "'"));
        try {
            List<Map<String, Object>>  queryResult = this.service.executeQuery(request);
            exchange.getResponseHeaders().put(io.undertow.util.Headers.CONTENT_TYPE, "application/json");
            exchange.setStatusCode(200);
            exchange.getResponseSender().send(JsonMapper.toJson(queryResult));
        } catch (Exception e) {
            logger.error("error happen: " + e);
            Status status = new Status(INVALID_KSQL_QUERY);
            status.setDescription(e.getMessage());
            setExchangeStatus(exchange, status);
        }
    }

}
