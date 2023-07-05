package ca.sunlife.eadp.eventhub.mesh.kafka.util;

import com.networknt.config.Config;
import com.networknt.kafka.common.KafkaStreamsConfig;
import com.networknt.kafka.entity.TopicReplayMetadata;
import com.networknt.service.SingletonServiceFactory;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

public class CustomSerdes {

    private CustomSerdes(){}

    static final SchemaRegistryClient client= SingletonServiceFactory.getBean(SchemaRegistryClient.class);
    static final Map<String,Object> kafkaStreamsConfig=((KafkaStreamsConfig) Config.getInstance().getJsonObjectConfig(KafkaStreamsConfig.CONFIG_NAME, KafkaStreamsConfig.class)).getProperties();
    public static final String VALUE_CLASS_NAME_CONFIG = "value.class.name";

    static final class TopicReplayMetadataSchemaSerde extends Serdes.WrapperSerde<TopicReplayMetadata> {
        TopicReplayMetadataSchemaSerde() {
            super(new KafkaJsonSchemaSerializer<>(client, kafkaStreamsConfig),
                    new KafkaJsonSchemaDeserializer<>(client,kafkaStreamsConfig));
        }
    }

    public static Serde<TopicReplayMetadata> topicReplayMetadataSerde() {

        TopicReplayMetadataSchemaSerde serde = new TopicReplayMetadataSchemaSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.putAll(kafkaStreamsConfig);
        serdeConfigs.put(CustomSerdes.VALUE_CLASS_NAME_CONFIG, TopicReplayMetadata.class);
        serdeConfigs.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, TopicReplayMetadata.class.getName());
        serde.configure(serdeConfigs, false);
        return serde;
    }

}
