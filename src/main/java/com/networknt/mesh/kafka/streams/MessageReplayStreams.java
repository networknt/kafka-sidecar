package com.networknt.mesh.kafka.streams;

import com.networknt.mesh.kafka.util.StreamsFactory;
import com.networknt.config.Config;
import com.networknt.kafka.common.KafkaStreamsConfig;
import com.networknt.kafka.streams.LightStreams;
import com.networknt.utility.ObjectUtils;
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

    static final KafkaStreamsConfig streamsConfig = (KafkaStreamsConfig) Config.getInstance().getJsonObjectConfig(KafkaStreamsConfig.CONFIG_NAME, KafkaStreamsConfig.class);

    private KafkaStreams kafkaStreams;

    @Override
    public void start(String ip, int port) {
        Properties streamProps=new Properties();
        streamsConfig.getProperties().put("auto.offset.reset", "latest");
        streamsConfig.getProperties().put(StreamsConfig.APPLICATION_ID_CONFIG, streamsConfig.getProperties().get("application.id").toString().concat("-replaystream"));
        streamsConfig.getProperties().put("enable.idempotence","false");
        streamProps.putAll(streamsConfig.getProperties());
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
            if(streamsConfig.isCleanUp()) {
                kafkaStreams.cleanUp();
            }
            kafkaStreams = startStream(ip, port, topology, streamsConfig, replayStreamsTopology.getDlqTopicMetadataMap(), MessageReplayStreamTopology.replayMetadataProcessor);

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
