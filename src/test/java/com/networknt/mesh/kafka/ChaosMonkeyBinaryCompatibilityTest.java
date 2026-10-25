package com.networknt.mesh.kafka;

import com.networknt.chaos.ChaosMonkeyPostHandler;
import com.networknt.chaos.ExceptionAssaultHandler;
import com.networknt.config.Config;
import com.networknt.httpstring.AttachmentConstants;
import io.undertow.io.Sender;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ChaosMonkeyBinaryCompatibilityTest {
    private static final String HANDLER = "com.networknt.chaos.ChaosMonkeyPostHandler";

    @Test
    void publishedJackson2HandlerFailsButRebuiltHandlerBinds() throws Exception {
        Path jar = Path.of("target/legacy/chaos-monkey-2.4.1.jar");
        assertTrue(Files.isRegularFile(jar), "Maven must copy the legacy release fixture");
        // SHA-256 of the release whose SHA-1 matches Maven Central; reject local rebuilds.
        assertEquals("e1d2b054a7d8f6390ca0999d2decd858c7cef96fcf5f2c6522a7585f57feefdc",
                HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(jar))));
        var config = Config.getInstance();
        Map<String, Object> previous = config.getJsonMapConfig("chaos-monkey");
        var previousAssault = ExceptionAssaultHandler.config;
        config.putInConfigCache("chaos-monkey", Map.of("enabled", true));
        try (var loader = new URLClassLoader(new URL[]{jar.toUri().toURL()}, getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (!HANDLER.equals(name)) return super.loadClass(name, resolve);
                Class<?> type = findLoadedClass(name);
                if (type == null) type = findClass(name);
                if (resolve) resolveClass(type);
                return type;
            }
        }) {
            var legacy = loader.loadClass(HANDLER).getConstructor().newInstance();
            var method = legacy.getClass().getMethod("handleRequest", HttpServerExchange.class);
            var failure = assertThrows(InvocationTargetException.class, () -> method.invoke(legacy, exchange()));
            var linkage = assertInstanceOf(NoSuchMethodError.class, failure.getCause());
            assertTrue(linkage.getMessage().contains("com.fasterxml.jackson.databind.ObjectMapper"), linkage::getMessage);
            assertDoesNotThrow(() -> new ChaosMonkeyPostHandler().handleRequest(exchange()));
        } finally {
            config.putInConfigCache("chaos-monkey", previous);
            ExceptionAssaultHandler.config = previousAssault;
        }
    }

    private HttpServerExchange exchange() {
        var exchange = mock(HttpServerExchange.class);
        var parameters = new TreeMap<String, Deque<String>>();
        parameters.put("assault", new ArrayDeque<>(List.of("com.networknt.chaos.ExceptionAssaultHandler")));
        when(exchange.getQueryParameters()).thenReturn(parameters);
        when(exchange.getAttachment(AttachmentConstants.REQUEST_BODY)).thenReturn(Map.of("enabled", false));
        when(exchange.getResponseHeaders()).thenReturn(new HeaderMap());
        when(exchange.getResponseSender()).thenReturn(mock(Sender.class));
        return exchange;
    }
}
