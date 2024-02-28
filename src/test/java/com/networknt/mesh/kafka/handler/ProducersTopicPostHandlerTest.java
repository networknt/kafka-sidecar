package com.networknt.mesh.kafka.handler;

import com.networknt.client.Http2Client;
import com.networknt.client.simplepool.SimpleConnectionHolder;
import com.networknt.exception.ClientException;
import com.networknt.mesh.kafka.TestServer;
import com.networknt.openapi.ResponseValidator;
import com.networknt.openapi.SchemaValidator;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.status.Status;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;


@Ignore
public class ProducersTopicPostHandlerTest {
    @ClassRule
    public static TestServer server = TestServer.getInstance();

    static final Logger logger = LoggerFactory.getLogger(ProducersTopicPostHandlerTest.class);
    static final boolean enableHttp2 = server.getServerConfig().isEnableHttp2();
    static final boolean enableHttps = server.getServerConfig().isEnableHttps();
    static final int httpPort = server.getServerConfig().getHttpPort();
    static final int httpsPort = server.getServerConfig().getHttpsPort();
    static final String url = enableHttp2 || enableHttps ? "https://localhost:" + httpsPort : "http://localhost:" + httpPort;
    static final String JSON_MEDIA_TYPE = "application/json";

    /**
     * This request body only contains the records with three elements. The format for key and value is JSON and
     * the key is a string and the value is a JSON object. The schema is defined in the schema registry with
     * id 1 and 2.
     *
     * @throws ClientException
     */
    @Test
    public void testJsonDefaultFormatProducerTopicPostHandlerTest() throws ClientException {

        final Http2Client client = Http2Client.getInstance();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ClientResponse> reference = new AtomicReference<>();
        String requestUri = "/producers/test1";
        String httpMethod = "post";
        String requestBody = "{\"key_schema_id\":1,\"value_schema_id\":2,\"records\":[{\"key\":\"alice\",\"value\":{\"count\":0}},{\"key\":\"john\",\"value\":{\"count\":1}},{\"key\":\"alex\",\"value\":{\"count\":2}}]}";
        SimpleConnectionHolder.ConnectionToken connectionToken = null;
        try {
            if(enableHttps) {
                connectionToken = client.borrow(new URI(url), Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, enableHttp2 ? OptionMap.create(UndertowOptions.ENABLE_HTTP2, true): OptionMap.EMPTY);
            } else {
                connectionToken = client.borrow(new URI(url), Http2Client.WORKER, Http2Client.BUFFER_POOL, OptionMap.EMPTY);
            }
            ClientConnection connection = (ClientConnection) connectionToken.getRawConnection();

            ClientRequest request = new ClientRequest().setPath(requestUri).setMethod(Methods.POST);

            request.getRequestHeaders().put(Headers.CONTENT_TYPE, JSON_MEDIA_TYPE);
            request.getRequestHeaders().put(Headers.TRANSFER_ENCODING, "chunked");
            //customized header parameters
            request.getRequestHeaders().put(new HttpString("host"), "localhost");
            connection.sendRequest(request, client.createClientCallback(reference, latch, requestBody));
            latch.await();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new ClientException(e);
        } finally {
            client.restore(connectionToken);
        }
        String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
        System.out.println("body = " +  body);;
        Optional<HeaderValues> contentTypeName = Optional.ofNullable(reference.get().getResponseHeaders().get(Headers.CONTENT_TYPE));
        SchemaValidator schemaValidator = new SchemaValidator();
        ResponseValidator responseValidator = new ResponseValidator(schemaValidator);
        int statusCode = reference.get().getResponseCode();
        System.out.println("statusCode = " + statusCode);
        Status status;
        if(contentTypeName.isPresent()) {
            status = responseValidator.validateResponseContent(body, requestUri, httpMethod, String.valueOf(statusCode), contentTypeName.get().getFirst());
        } else {
            status = responseValidator.validateResponseContent(body, requestUri, httpMethod, String.valueOf(statusCode), JSON_MEDIA_TYPE);
        }
        Assert.assertNull(status);
    }

    /**
     * This request body only contains the records with three elements. The format for key and value is JSON and
     * the key is a string and the value is a JSON object. The schema is defined in the schema registry with
     * id 1 and 2.
     *
     * @throws ClientException
     */
    @Test
    public void testJsonWithFormatProducerTopicPostHandlerTest() throws ClientException {

        final Http2Client client = Http2Client.getInstance();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ClientResponse> reference = new AtomicReference<>();
        String requestUri = "/producers/test1";
        String httpMethod = "post";
        String requestBody = "{\"format\":3,\"key_schema_id\":1,\"value_schema_id\":2,\"records\":[{\"key\":\"alice\",\"value\":{\"count\":0}},{\"key\":\"john\",\"value\":{\"count\":1}},{\"key\":\"alex\",\"value\":{\"count\":2}}]}";
        SimpleConnectionHolder.ConnectionToken connectionToken = null;

        try {
            if(enableHttps) {
                connectionToken = client.borrow(new URI(url), Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, enableHttp2 ? OptionMap.create(UndertowOptions.ENABLE_HTTP2, true): OptionMap.EMPTY);
            } else {
                connectionToken = client.borrow(new URI(url), Http2Client.WORKER, Http2Client.BUFFER_POOL, OptionMap.EMPTY);
            }
            ClientConnection connection = (ClientConnection) connectionToken.getRawConnection();

            ClientRequest request = new ClientRequest().setPath(requestUri).setMethod(Methods.POST);

            request.getRequestHeaders().put(Headers.CONTENT_TYPE, JSON_MEDIA_TYPE);
            request.getRequestHeaders().put(Headers.TRANSFER_ENCODING, "chunked");
            //customized header parameters
            request.getRequestHeaders().put(new HttpString("host"), "localhost");
            connection.sendRequest(request, client.createClientCallback(reference, latch, requestBody));
            latch.await();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new ClientException(e);
        } finally {
            client.restore(connectionToken);
        }
        String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
        System.out.println("body = " +  body);;
        Optional<HeaderValues> contentTypeName = Optional.ofNullable(reference.get().getResponseHeaders().get(Headers.CONTENT_TYPE));
        SchemaValidator schemaValidator = new SchemaValidator();
        ResponseValidator responseValidator = new ResponseValidator(schemaValidator);
        int statusCode = reference.get().getResponseCode();
        System.out.println("statusCode = " + statusCode);
        Status status;
        if(contentTypeName.isPresent()) {
            status = responseValidator.validateResponseContent(body, requestUri, httpMethod, String.valueOf(statusCode), contentTypeName.get().getFirst());
        } else {
            status = responseValidator.validateResponseContent(body, requestUri, httpMethod, String.valueOf(statusCode), JSON_MEDIA_TYPE);
        }
        Assert.assertNull(status);
    }

    /**
     * This request body only contains the records with three elements. The format for key and value is AVRO and
     * the key is a string and the value is an object. The schema is defined in the schema registry with
     * id 3 and 4.
     *
     * @throws ClientException
     */
    @Test
    public void testAvroProducerTopicPostHandlerTest() throws ClientException {

        final Http2Client client = Http2Client.getInstance();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ClientResponse> reference = new AtomicReference<>();
        String requestUri = "/producers/test2";
        String httpMethod = "post";
        String requestBody = "{\"format\":2,\"key_schema_id\":3,\"value_schema_id\":5,\"records\":[{\"key\":\"Alex\",\"value\":{\"count\":0}},{\"key\":\"Alice\",\"value\":{\"count\":1}},{\"key\":\"Bob\",\"value\":{\"count\":2}}]}";
        SimpleConnectionHolder.ConnectionToken connectionToken = null;

        try {
            if(enableHttps) {
                connectionToken = client.borrow(new URI(url), Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, enableHttp2 ? OptionMap.create(UndertowOptions.ENABLE_HTTP2, true): OptionMap.EMPTY);
            } else {
                connectionToken = client.borrow(new URI(url), Http2Client.WORKER, Http2Client.BUFFER_POOL, OptionMap.EMPTY);
            }
            ClientConnection connection = (ClientConnection) connectionToken.getRawConnection();

            ClientRequest request = new ClientRequest().setPath(requestUri).setMethod(Methods.POST);

            request.getRequestHeaders().put(Headers.CONTENT_TYPE, JSON_MEDIA_TYPE);
            request.getRequestHeaders().put(Headers.TRANSFER_ENCODING, "chunked");
            //customized header parameters
            request.getRequestHeaders().put(new HttpString("host"), "localhost");
            connection.sendRequest(request, client.createClientCallback(reference, latch, requestBody));
            latch.await();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new ClientException(e);
        } finally {
            client.restore(connectionToken);
        }
        String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
        System.out.println("body = " +  body);;
        Optional<HeaderValues> contentTypeName = Optional.ofNullable(reference.get().getResponseHeaders().get(Headers.CONTENT_TYPE));
        SchemaValidator schemaValidator = new SchemaValidator();
        ResponseValidator responseValidator = new ResponseValidator(schemaValidator);
        int statusCode = reference.get().getResponseCode();
        System.out.println("statusCode = " + statusCode);
        Status status;
        if(contentTypeName.isPresent()) {
            status = responseValidator.validateResponseContent(body, requestUri, httpMethod, String.valueOf(statusCode), contentTypeName.get().getFirst());
        } else {
            status = responseValidator.validateResponseContent(body, requestUri, httpMethod, String.valueOf(statusCode), JSON_MEDIA_TYPE);
        }
        Assert.assertNull(status);
    }

    /**
     * This request body only contains the records with three elements. The format for key and value is Protobuf and
     * the key is a string and the value is an object. The schema is defined in the schema registry with
     * id 5 and 6.
     *
     * @throws ClientException
     */
    @Test
    public void testProtobufProducerTopicPostHandlerTest() throws ClientException {

        final Http2Client client = Http2Client.getInstance();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ClientResponse> reference = new AtomicReference<>();
        String requestUri = "/producers/test3";
        String httpMethod = "post";
        String requestBody = "{\"format\":4,\"key_schema_id\":6,\"value_schema_id\":7,\"records\":[{\"key\":{\"name\":\"Steve\"},\"value\":{\"count\":1,\"name\":\"Steve\"}}]}";
        SimpleConnectionHolder.ConnectionToken connectionToken = null;

        try {
            if(enableHttps) {
                connectionToken = client.borrow(new URI(url), Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, enableHttp2 ? OptionMap.create(UndertowOptions.ENABLE_HTTP2, true): OptionMap.EMPTY);
            } else {
                connectionToken = client.borrow(new URI(url), Http2Client.WORKER, Http2Client.BUFFER_POOL, OptionMap.EMPTY);
            }
            ClientConnection connection = (ClientConnection) connectionToken.getRawConnection();

            ClientRequest request = new ClientRequest().setPath(requestUri).setMethod(Methods.POST);

            request.getRequestHeaders().put(Headers.CONTENT_TYPE, JSON_MEDIA_TYPE);
            request.getRequestHeaders().put(Headers.TRANSFER_ENCODING, "chunked");
            //customized header parameters
            request.getRequestHeaders().put(new HttpString("host"), "localhost");
            connection.sendRequest(request, client.createClientCallback(reference, latch, requestBody));
            latch.await();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new ClientException(e);
        } finally {
            client.restore(connectionToken);
        }
        String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
        System.out.println("body = " +  body);;
        Optional<HeaderValues> contentTypeName = Optional.ofNullable(reference.get().getResponseHeaders().get(Headers.CONTENT_TYPE));
        SchemaValidator schemaValidator = new SchemaValidator();
        ResponseValidator responseValidator = new ResponseValidator(schemaValidator);
        int statusCode = reference.get().getResponseCode();
        System.out.println("statusCode = " + statusCode);
        Status status;
        if(contentTypeName.isPresent()) {
            status = responseValidator.validateResponseContent(body, requestUri, httpMethod, String.valueOf(statusCode), contentTypeName.get().getFirst());
        } else {
            status = responseValidator.validateResponseContent(body, requestUri, httpMethod, String.valueOf(statusCode), JSON_MEDIA_TYPE);
        }
        Assert.assertNull(status);
    }

}
