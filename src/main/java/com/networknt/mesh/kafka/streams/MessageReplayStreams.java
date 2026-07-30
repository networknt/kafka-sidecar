package com.networknt.mesh.kafka.streams;

import com.networknt.mesh.kafka.util.StreamsFactory;
import com.networknt.kafka.common.KafkaStreamsConfig;
import com.networknt.kafka.streams.LightStreams;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MessageReplayStreams implements LightStreams {

    private static final Logger logger= LoggerFactory.getLogger(MessageReplayStreams.class);

    static final KafkaStreamsConfig replayStreamsConfig = KafkaStreamsConfig.load();

    private KafkaStreams kafkaStreams;

    @Override
    public void start(String ip, int port) {
        Properties streamProps = new Properties();
        streamProps.putAll(replayStreamsConfig.getKafkaMapProperties());
        Object applicationId = streamProps.get(StreamsConfig.APPLICATION_ID_CONFIG);
        if (applicationId == null) {
            throw new IllegalStateException("Kafka Streams application.id must be configured for message replay");
        }
        // Apply replay-specific overrides after the base properties so they take precedence.
        streamProps.put("auto.offset.reset", "latest");
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG,
                applicationId.toString().concat("-replaystream"));
        streamProps.put("enable.idempotence","false");
        streamProps.put(StreamsConfig.APPLICATION_SERVER_CONFIG, ip +":"+port);
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        MessageReplayStreamTopology replayStreamsTopology = new MessageReplayStreamTopology();
        Topology topology = replayStreamsTopology.buildReplayTopology();

        try {
            kafkaStreams = StreamsFactory.createKafkaStreams(topology, streamProps);
            kafkaStreams.setUncaughtExceptionHandler(eh ->{
                logger.error("Kafka-Streams uncaught exception occurred. Stream will be replaced with new thread", eh);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
            });
            if(replayStreamsConfig.getCleanUp()) {
                kafkaStreams.cleanUp();
            }
            kafkaStreams = startStream(ip, port, topology, replayStreamsConfig, replayStreamsTopology.getDlqTopicMetadataMap(), MessageReplayStreamTopology.replayMetadataProcessor);

        }catch (Exception e){
            logger.error(e.getMessage());
            kafkaStreams = null;
        }
    }

    @Override
    public void close() {
        if(kafkaStreams !=null)
            kafkaStreams.close();
    }
}
