package com.networknt.mesh.kafka.handler;

import com.networknt.body.BodyHandler;
import com.networknt.client.Http2Client;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.handler.LightHttpHandler;
import com.networknt.health.HealthConfig;
import com.networknt.health.HealthGetHandler;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.entity.CreateConsumerInstanceRequest;
import com.networknt.kafka.entity.CreateConsumerInstanceResponse;
import com.networknt.mesh.kafka.ActiveConsumerStartupHook;
import com.networknt.mesh.kafka.ProducerStartupHook;
import com.networknt.mesh.kafka.ReactiveConsumerStartupHook;
import com.networknt.server.StartupHookProvider;
import com.networknt.service.SingletonServiceFactory;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.OptionMap;

import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * The Kafka sidecar specific health check handler return OK only when all enabled components are created
 * successfully and connect to the backend API if Reactive Consumer is enabled. Please be aware that due to
 * the nature of the Kafka client, we cannot ensure that the connection to the Kafka is successful or not.
 * In order to check the connectivity to the Kafka cluster, the only way for producer is to send a dummy
 * record to a dummy topic and for consumer is to consume records from a dummy topic. This seems too heavy
 * for health check when the heath check endpoint is called by the controller every a few seconds. Given
 * the reasons above, we are just check the producer and consumerManager object successfully created in the
 * startup hooks for the producer and consumer when they are enabled.
 *
 * As for the backend API, it needs to implement a /health endpoint and return "OK" with 200 status code.
 * When the sidecar health check endpoint is invoked, it will send a request to the backend API health
 * check endpoint to combine both statuses to decide if the sidecar is healthy.
 *
 * @author Steve Hu
 */
public class SidecarHealthHandler implements LightHttpHandler {

    public static final String HEALTH_RESULT_OK = "OK";
    public static final String HEALTH_RESULT_ERROR = "ERROR";
    static final Logger logger = LoggerFactory.getLogger(SidecarHealthHandler.class);
    static final KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        String result = HEALTH_RESULT_OK;
        // using startup hooks to detect which component is enabled.
        StartupHookProvider[] startupHookProviders = SingletonServiceFactory.getBeans(StartupHookProvider.class);
        if (startupHookProviders != null && startupHookProviders.length > 0) {
            for(StartupHookProvider p : startupHookProviders) {
                if(p instanceof ProducerStartupHook) {
                    // producer is enabled.
                    if (ProducerStartupHook.producer == null) {
                       logger.error("Producer is enabled but it is not connected to the Kafka cluster.");
                        result = HEALTH_RESULT_ERROR;
                    }
                }
                if(p instanceof ReactiveConsumerStartupHook) {
                    // reactive consumer is enabled, need to check backend connection as well.
                    if(ReactiveConsumerStartupHook.kafkaConsumerManager == null) {
                        logger.error("ReactiveConsumer is enabled but it is not connected to the Kafka cluster.");
                        result = HEALTH_RESULT_ERROR;
                    }
                    // if backend is not connected, then error.
                    result = backendHealth();
                }
            }
        } else {
            logger.error("No startup hook is defined and none of the component is enabled.");
            result = HEALTH_RESULT_ERROR;
        }
        // for security reason, we don't output the details about the error. Users can check the log for the failure.
        if(HEALTH_RESULT_ERROR == result) {
            exchange.setStatusCode(400);
            exchange.getResponseSender().send(HEALTH_RESULT_ERROR);
        } else {
            exchange.setStatusCode(200);
            exchange.getResponseSender().send(HEALTH_RESULT_OK);
        }
    }

    /**
     * Try to access the /health endpoint on the backend API. return OK if a success response is returned. Otherwise,
     * ERROR is returned.
     *
     * @return result String of OK or ERROR.
     */
    private String backendHealth() {
        String result = HEALTH_RESULT_OK;
        ClientConnection connection = ReactiveConsumerStartupHook.connection;
        if(connection == null || !connection.isOpen()) {
            try {
                if(config.getBackendApiHost().startsWith("https")) {
                    connection = ReactiveConsumerStartupHook.client.borrowConnection(new URI(config.getBackendApiHost()), Http2Client.WORKER, ReactiveConsumerStartupHook.client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true)).get();
                } else {
                    connection = ReactiveConsumerStartupHook.client.borrowConnection(new URI(config.getBackendApiHost()), Http2Client.WORKER, Http2Client.BUFFER_POOL, OptionMap.EMPTY).get();
                }
            } catch (Exception ex) {
                logger.error("Could not create connection to the backend:", ex);
                result = HEALTH_RESULT_ERROR;
            }
        }
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ClientResponse> reference = new AtomicReference<>();
        try {
            ClientRequest request = new ClientRequest().setMethod(Methods.GET).setPath("/health");
            request.getRequestHeaders().put(Headers.HOST, "localhost");
            connection.sendRequest(request, ReactiveConsumerStartupHook.client.createClientCallback(reference, latch));
            latch.await();
            int statusCode = reference.get().getResponseCode();
            String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
            if(logger.isDebugEnabled()) logger.debug("statusCode = " + statusCode + " body  = " + body);
            if(statusCode >= 400) {
                // something happens on the backend and the health check is not respond.
                logger.error("Error due to error response from backend with status code = " + statusCode + " body = " + body);
                result = HEALTH_RESULT_ERROR;
            }
        } catch (Exception exception) {
            logger.error("Error while sending a health check request to the backend with exception: ", exception);
            result = HEALTH_RESULT_ERROR;
        }
        return result;
    }
}
