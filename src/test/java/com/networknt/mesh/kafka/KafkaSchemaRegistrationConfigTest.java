package com.networknt.mesh.kafka;

import com.networknt.config.ConfigInjection;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class KafkaSchemaRegistrationConfigTest {
    @Test
    void everyShippedConfigurationUsesCanonicalOverridesAndRegistrationBehavior() throws Exception {
        Map<String, Object> original = ConfigInjection.getDecryptedValueMap();
        Pattern setting = Pattern.compile("(?m)^  auto\\.register\\.schemas: (\\$\\{(kafka-(?:producer|consumer|streams))\\.auto\\.register\\.schemas:(true|false)})$");
        int count = 0;
        try {
            for (Path root : List.of(Path.of("config"), Path.of("src/main/resources/config"))) {
                try (var paths = Files.walk(root)) {
                    for (Path path : paths.filter(p -> p.getFileName().toString().matches("kafka-(producer|consumer|streams)\\.yml")).toList()) {
                        String yaml = Files.readString(path);
                        assertFalse(yaml.contains("schema.registry.auto.register.schemas"), path.toString());
                        var match = setting.matcher(yaml);
                        assertTrue(match.find(), path.toString());
                        count++;
                        String placeholder = match.group(1);
                        String client = match.group(2);
                        boolean defaultValue = Boolean.parseBoolean(match.group(3));
                        Map<String, Object> overrides = new HashMap<>();
                        overrides.put(client + ".schema.registry.auto.register.schemas", !defaultValue);
                        ConfigInjection.setDecryptedValueMap(overrides);
                        assertEquals(defaultValue, ConfigInjection.getInjectValue(placeholder, true), path.toString());
                        for (boolean register : List.of(true, false)) {
                            overrides.put(client + ".auto.register.schemas", register);
                            ConfigInjection.setDecryptedValueMap(overrides);
                            Object injected = ConfigInjection.getInjectValue(placeholder, true);
                            assertEquals(register, injected, path.toString());
                            MockSchemaRegistryClient registry = new MockSchemaRegistryClient();
                            Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ConfigRegistration\",\"fields\":[]}");
                            GenericData.Record record = new GenericData.Record(schema);
                            try (KafkaAvroSerializer serializer = new KafkaAvroSerializer(registry)) {
                                serializer.configure(Map.of("schema.registry.url", "mock://config-registration",
                                        "auto.register.schemas", injected), false);
                                if (!register) {
                                    assertThrows(SerializationException.class, () -> serializer.serialize("events", record));
                                    RestClientException missing = assertThrows(RestClientException.class,
                                            () -> registry.getAllVersions("events-value"));
                                    assertEquals(40401, missing.getErrorCode());
                                    registry.register("events-value", new AvroSchema(schema));
                                }
                                assertNotNull(serializer.serialize("events", record));
                                assertEquals(1, registry.getAllSubjects().size());
                            }
                        }
                    }
                }
            }
            assertEquals(16, count, "Audit all shipped producer, consumer and Streams configurations");
        } finally {
            ConfigInjection.setDecryptedValueMap(original);
        }
    }
}
