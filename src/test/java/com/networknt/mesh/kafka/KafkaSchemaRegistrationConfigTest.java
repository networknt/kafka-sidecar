package com.networknt.mesh.kafka;

import com.networknt.config.Config;
import com.networknt.config.ConfigInjection;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.common.KafkaProducerConfig;
import com.networknt.kafka.common.KafkaStreamsConfig;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

@Isolated("Changes the shared configuration values and cache")
class KafkaSchemaRegistrationConfigTest {
    private static final Path ROOT = Path.of(System.getProperty("basedir"));
    private static final String KEY = "auto.register.schemas";
    private static final String LEGACY = "schema.registry.auto.register.schemas";
    private static final String PACKAGED_CONSUMER = "src/main/resources/config/kafka-consumer.yml";
    private static final List<String> CONFIGURATIONS = List.of(
            "config/docker/kafka-consumer.yml",
            "config/docker/kafka-producer.yml",
            "config/local/kafka-consumer.yml",
            "config/local/kafka-producer.yml",
            "config/local/kafka-streams.yml",
            "config/producer/kafka-producer.yml",
            "config/producer/kafka-streams.yml",
            "config/reactive-consumer-producer/kafka-consumer.yml",
            "config/reactive-consumer-producer/kafka-producer.yml",
            "config/reactive-consumer-producer/kafka-streams.yml",
            "config/streams/kafka-consumer.yml",
            "config/streams/kafka-producer.yml",
            "config/streams/kafka-streams.yml",
            PACKAGED_CONSUMER,
            "src/main/resources/config/kafka-producer.yml",
            "src/main/resources/config/kafka-streams.yml");

    @Test
    void inventoryIncludesEveryShippedClientConfiguration() throws Exception {
        Set<String> discovered = new HashSet<>();
        for (String directory : List.of("config", "src/main/resources/config")) {
            try (var paths = Files.walk(ROOT.resolve(directory))) {
                paths.filter(path -> path.getFileName().toString()
                                .matches("kafka-(producer|consumer|streams)\\.ya?ml"))
                        .map(ROOT::relativize).map(Path::toString).forEach(discovered::add);
            }
        }
        assertEquals(Set.copyOf(CONFIGURATIONS), discovered,
                "Update the explicit default expectations when adding or removing a shipped config");
    }

    @TestFactory
    Stream<DynamicNode> shippedConfigurationsReachTypedKafkaProperties() {
        return CONFIGURATIONS.stream().map(file -> dynamicContainer(file, Stream.of(
                dynamicTest("explicit default", () -> assertRegistration(file, Map.of(),
                        !PACKAGED_CONSUMER.equals(file))),
                dynamicTest("existing override false", () -> assertOverride(file, false)),
                dynamicTest("existing override true", () -> assertOverride(file, true)))));
    }

    private void assertOverride(String file, boolean value) {
        String client = Path.of(file).getFileName().toString().replace(".yml", "");
        assertRegistration(file, Map.of(client + "." + LEGACY, value), value);
    }

    private void assertRegistration(String file, Map<String, Object> overrides, boolean expected) {
        Map<String, Object> original = ConfigInjection.getDecryptedValueMap();
        String client = Path.of(file).getFileName().toString().replace(".yml", "");
        Config config = Config.getInstance();
        try {
            ConfigInjection.setDecryptedValueMap(new HashMap<>(overrides));
            config.clearConfigCache(client);
            // Load this exact shipped file through YAML parsing and values injection before
            // the typed loader consumes the cached map, rather than a classpath fallback.
            assertNotNull(config.getJsonMapConfig(client, ROOT.resolve(file).getParent().toString()));
            Map<String, Object> properties = switch (client) {
                case "kafka-producer" -> KafkaProducerConfig.load().getProperties();
                case "kafka-consumer" -> KafkaConsumerConfig.load().getProperties();
                case "kafka-streams" -> KafkaStreamsConfig.load().getProperties();
                default -> throw new IllegalArgumentException(client);
            };
            assertEquals(expected, properties.get(KEY), file);
            assertFalse(properties.containsKey(LEGACY), file);
        } finally {
            config.clearConfigCache(client);
            ConfigInjection.setDecryptedValueMap(original);
        }
    }
}
