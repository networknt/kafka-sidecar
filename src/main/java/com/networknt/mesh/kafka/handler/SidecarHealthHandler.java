package com.networknt.mesh.kafka.handler;

import com.networknt.client.Http2Client;
import com.networknt.client.simplepool.SimpleConnectionHolder;
import com.networknt.config.Config;
import com.networknt.handler.LightHttpHandler;
import com.networknt.health.HealthConfig;
import com.networknt.mesh.kafka.AdminClientStartupHook;
import com.networknt.mesh.kafka.ProducerStartupHook;
import com.networknt.mesh.kafka.ReactiveConsumerStartupHook;
import com.networknt.server.StartupHookProvider;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.utility.Constants;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.OptionMap;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
    static final HealthConfig config = (HealthConfig) Config.getInstance().getJsonObjectConfig(HealthConfig.CONFIG_NAME, HealthConfig.class);
    // cached connection to the backend API to speed up the downstream check.
    public static Http2Client client = Http2Client.getInstance();

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        String result = HEALTH_RESULT_OK;
        // using startup hooks to detect which component is enabled.
        StartupHookProvider[] startupHookProviders = SingletonServiceFactory.getBeans(StartupHookProvider.class);
        if (startupHookProviders != null && startupHookProviders.length > 0) {
            for(StartupHookProvider p : startupHookProviders) {
                if(p instanceof ProducerStartupHook && ProducerStartupHook.producer == null) {
                    // producer is enabled.
                    logger.error("Producer is enabled but it is not connected to the Kafka cluster.");
                    result = HEALTH_RESULT_ERROR;
                }
                if(p instanceof ReactiveConsumerStartupHook) {
                    // reactive consumer is enabled, but consumer manager is null or the healthy is false, return error.
                    if(ReactiveConsumerStartupHook.kafkaConsumerManager == null || !ReactiveConsumerStartupHook.healthy) {
                        logger.error("ReactiveConsumer is enabled but it is not connected to the Kafka cluster or it is marked as unhealthy.");
                        result = HEALTH_RESULT_ERROR;
                    }
                    // if backend is not connected, then error. Check the configuration to see if it is enabled.
                    // skip this check if the result is an error already.
                    if(HEALTH_RESULT_OK.equals(result) && config.isDownstreamEnabled()) {
                        result = backendHealth();
                    }
                }
                if(p instanceof AdminClientStartupHook) {
                    if(AdminClientStartupHook.admin == null) {
                        logger.error("AdminClient is enabled but it is not connected to the Kafka cluster.");
                        result = HEALTH_RESULT_ERROR;
                    } else {
                        result = kafkaHealth();
                    }
                }
                // if there is any error in the loop, don't need to check further.
                if(result.equals(HEALTH_RESULT_ERROR)) break;
            }
        } else {
            logger.error("No startup hook is defined and none of the component is enabled.");
            result = HEALTH_RESULT_ERROR;
        }
        // for security reason, we don't output the details about the error. Users can check the log for the failure.
        if(HEALTH_RESULT_ERROR.equals(result)) {
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
    public static String backendHealth() {
        String result = HEALTH_RESULT_OK;
        long start = System.currentTimeMillis();
        SimpleConnectionHolder.ConnectionToken connectionToken = null;
        try {
            if(config.getDownstreamHost().startsWith(Constants.HTTPS)) {
                connectionToken = client.borrow(new URI(config.getDownstreamHost()), Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true));
            } else {
                connectionToken = client.borrow(new URI(config.getDownstreamHost()), Http2Client.WORKER, Http2Client.BUFFER_POOL, OptionMap.EMPTY);
            }
            ClientConnection connection = (ClientConnection) connectionToken.getRawConnection();
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<ClientResponse> reference = new AtomicReference<>();
            try {
                ClientRequest request = new ClientRequest().setMethod(Methods.GET).setPath(config.getDownstreamPath());
                request.getRequestHeaders().put(Headers.HOST, "localhost");
                if(logger.isDebugEnabled()) logger.debug("Header information printed in HealthCheck {}", request.getRequestHeaders().toString());
                connection.sendRequest(request, ReactiveConsumerStartupHook.client.createClientCallback(reference, latch));
                latch.await(config.getTimeout(), TimeUnit.MILLISECONDS);
                int statusCode = reference.get().getResponseCode();
                String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
                if(logger.isDebugEnabled()) logger.debug("statusCode = {} body  = {}", statusCode, body);
                if(statusCode >= 400) {
                    // something happens on the backend and the health check is not respond.
                    logger.error("Error due to error response from backend with status code = {} body = {}", statusCode, body);
                    result = HEALTH_RESULT_ERROR;
                }
            } catch (Exception exception) {
                logger.error("Error while sending a health check request to the backend with exception: ", exception);
                // for Java EE backend like spring boot, the connection created and opened but might not ready. So we need to close
                // the connection if there are any exception here to work around the spring boot backend.
                connectionToken.holder().safeClose(System.currentTimeMillis());
                result = HEALTH_RESULT_ERROR;
            }
            long responseTime = System.currentTimeMillis() - start;
            if(logger.isDebugEnabled()) logger.debug("Downstream health check response time = {}", responseTime);
            return result;
        } catch (Exception ex) {
            logger.error("Could not create connection to the backend: " + config.getDownstreamHost() + ":", ex);
            result = HEALTH_RESULT_ERROR;
            // if connection cannot be established, return error. The backend is not started yet.
            return result;
        } finally {
            client.restore(connectionToken);
        }
    }

    /**
     * Check the Kafka Cluster with Admin Client. Return OK if all three checks are passed. Otherwise, return
     * error. This is only triggered if AdminClientStartupHook is enabled in the service.yml config file.
     *
     * @return OK if all checks are passed.
     */
    private String kafkaHealth() {
        String result = HEALTH_RESULT_OK;
        try {
            final DescribeClusterResult response = AdminClientStartupHook.admin.describeCluster();
            final boolean nodesNotEmpty = !response.nodes().get(config.getTimeout(), TimeUnit.MILLISECONDS).isEmpty();
            final boolean clusterIdAvailable = response.clusterId() != null;
            final boolean controllerExists = response.controller().get(config.getTimeout(), TimeUnit.MILLISECONDS) != null;

            if (!nodesNotEmpty) {
                logger.error("no nodes found for the Kafka Cluster");
                result = HEALTH_RESULT_ERROR;
            }

            if (!clusterIdAvailable) {
                logger.error("no cluster id available for the Kafka Cluster");
                result = HEALTH_RESULT_ERROR;
            }

            if (!controllerExists) {
                logger.error("no active controller exists for the Kafka Cluster");
                result = HEALTH_RESULT_ERROR;
            }
        } catch (Exception e) {
            logger.error("Error describing Kafka Cluster", e);
            result = HEALTH_RESULT_ERROR;
        }
        return result;
    }
}
