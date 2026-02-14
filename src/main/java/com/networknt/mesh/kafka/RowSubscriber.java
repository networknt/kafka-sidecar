package com.networknt.mesh.kafka;

import com.networknt.client.Http2Client;
import com.networknt.client.simplepool.SimpleConnectionState;
import com.networknt.config.Config;
import com.networknt.kafka.common.KafkaKsqldbConfig;
import com.networknt.utility.Constants;
import io.confluent.ksql.api.client.Row;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.OptionMap;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class RowSubscriber implements Subscriber<Row> {
    private static final Logger logger = LoggerFactory.getLogger(RowSubscriber.class);
    static Http2Client client = Http2Client.getInstance();
    private ClientConnection connection;
    private Subscription subscription;
    private KafkaKsqldbConfig config = (KafkaKsqldbConfig) Config.getInstance().getJsonObjectConfig(KafkaKsqldbConfig.CONFIG_NAME, KafkaKsqldbConfig.class);

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
        logger.info("Subscriber is subscribed.");
        this.subscription = subscription;
        // Request the first row
        subscription.request(1);
    }

    @Override
    public synchronized void onNext(Row row) {
        logger.info("Received a row!");
        logger.info("Row: {}", row.values());
        // send the row to the ksqldb-backend instance

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ClientResponse> reference = new AtomicReference<>();

        SimpleConnectionState.ConnectionToken connectionToken = null;

        try {
            if (config.getBackendUrl().startsWith(Constants.HTTPS)) {
                connectionToken = client.borrow(new URI(config.getBackendUrl()), Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true));
            } else {
                connectionToken = client.borrow(new URI(config.getBackendUrl()), Http2Client.WORKER, Http2Client.BUFFER_POOL, OptionMap.EMPTY);
            }
            ClientConnection connection = (ClientConnection) connectionToken.getRawConnection();
            ClientRequest request = new ClientRequest().setMethod(Methods.POST).setPath(config.getBackendPath());
            request.getRequestHeaders().put(Headers.CONTENT_TYPE, "application/json");
            request.getRequestHeaders().put(Headers.TRANSFER_ENCODING, "chunked");
            connection.sendRequest(request, client.createClientCallback(reference, latch, row.values().toString()));
            latch.await();
            int statusCode = reference.get().getResponseCode();
            String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
            logger.debug("statusCode = {}", statusCode);
            logger.debug("body = {}", body);
        } catch (Exception e) {
            logger.error("exception thrown in onNext send request", e);
        } finally {
            client.restore(connectionToken);
        }
        // Request the next row
        subscription.request(1);
    }

    @Override
    public synchronized void onError(Throwable t) {
        logger.info("Received an error: ", t);
    }

    @Override
    public synchronized void onComplete() {
        logger.info("Query has ended.");
    }
}
