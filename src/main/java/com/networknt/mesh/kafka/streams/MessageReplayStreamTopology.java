package com.networknt.mesh.kafka.streams;

import com.networknt.mesh.kafka.util.CustomSerdes;
import com.networknt.config.Config;
import com.networknt.kafka.common.config.KafkaStreamsConfig;
import com.networknt.kafka.entity.StreamsDLQMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MessageReplayStreamTopology {

    private static final Logger logger = LoggerFactory.getLogger(MessageReplayStreamTopology.class);
    static final KafkaStreamsConfig kafkaStreamsConfig= KafkaStreamsConfig.load();
    private KafkaStreams kafkaStreams;

    public static final String replayMetadataSrc= "replay-source";
    public static final String replayMetadataProcessor= "replay-processor";

    private Map<String, StreamsDLQMetadata> dlqTopicMetadataMap;


    public Topology buildReplayTopology() {

        final Topology builder = new Topology();
        dlqTopicMetadataMap= new HashMap<>();

        builder.addSource(replayMetadataSrc,
                Serdes.String().deserializer(),
                CustomSerdes.topicReplayMetadataSerde().deserializer(),
                kafkaStreamsConfig.getDeadLetterControllerTopic());

        builder.addProcessor(replayMetadataProcessor, MessageReplayProcessor::new, replayMetadataSrc);

        StreamsDLQMetadata replayDlqMetadata= new StreamsDLQMetadata();
        replayDlqMetadata.setSerde(CustomSerdes.topicReplayMetadataSerde());
        replayDlqMetadata.setParentNames(Arrays.asList(replayMetadataProcessor ));

        dlqTopicMetadataMap.put(kafkaStreamsConfig.getDeadLetterControllerTopic().trim().concat(".dlq"), replayDlqMetadata);

        return builder;

    }

    public Map<String, StreamsDLQMetadata> getDlqTopicMetadataMap() {
        return dlqTopicMetadataMap;
    }

}
