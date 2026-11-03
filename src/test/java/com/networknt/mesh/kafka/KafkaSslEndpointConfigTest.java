package com.networknt.mesh.kafka;

import com.networknt.config.Config;
import com.networknt.config.ConfigInjection;
import com.networknt.kafka.common.KafkaAdminConfig;
import com.networknt.kafka.common.KafkaProducerConfig;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.common.KafkaStreamsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Isolated("Temporarily changes shared configuration injection values")
class KafkaSslEndpointConfigTest {
    private static final String KEY = "ssl.endpoint.identification.algorithm";
    private static final Path ROOT = Path.of(System.getProperty("basedir"));

    @Test
    void everyShippedTlsExampleUsesHttpsAndPackagedClientsPreserveExplicitOverrides() throws Exception {
        int count = 0;
        for (String directory : List.of("config", "src/main/resources/config")) {
            try (var paths = Files.walk(ROOT.resolve(directory))) {
                for (Path file : paths.filter(p -> p.getFileName().toString().matches("kafka-.*\\.yml")).toList()) {
                    String yaml = Files.readString(file);
                    if (!yaml.contains(KEY)) continue;
                    String client = file.getFileName().toString().replace(".yml", "");
                    assertTrue(yaml.contains("${" + client + "." + KEY + ":https}"), file.toString());
                    assertFalse(yaml.contains(":algo-name}"), file.toString());
                    count++;
                }
            }
        }
        assertEquals(21, count, "Review new/removed shipped TLS configs explicitly");
        Map<String, Object> original = ConfigInjection.getDecryptedValueMap();
        try {
            for (String client : List.of("kafka-admin", "kafka-producer", "kafka-consumer", "kafka-streams")) {
                ConfigInjection.setDecryptedValueMap(new HashMap<>());
                assertEquals("https", properties(client).get(KEY), client);
                for (String value : List.of("", "https")) {
                    ConfigInjection.setDecryptedValueMap(new HashMap<>(Map.of(client + "." + KEY, value)));
                    assertEquals(value, properties(client).get(KEY), client);
                }
            }
        } finally {
            ConfigInjection.setDecryptedValueMap(original);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> properties(String client) {
        Map<String, Object> source = Config.getInstance().getJsonMapConfigNoCache(client,
                ROOT.resolve("src/main/resources/config").toString());
        assertNotNull(source, client);
        Map<String, Object> values = (Map<String, Object>) source.get("properties");
        return switch (client) {
            case "kafka-admin" -> { var config = new KafkaAdminConfig(); config.setProperties(values); yield config.getProperties(); }
            case "kafka-producer" -> { var config = new KafkaProducerConfig(); config.setProperties(values); yield config.getProperties(); }
            case "kafka-consumer" -> { var config = new KafkaConsumerConfig(); config.setProperties(values); yield config.getProperties(); }
            case "kafka-streams" -> { var config = new KafkaStreamsConfig(); config.setProperties(values); yield config.getProperties(); }
            default -> throw new IllegalArgumentException(client);
        };
    }
}
