package ca.sunlife.eadp.eventhub.mesh.kafka.util;

import ca.sunlife.eadp.eventhub.mesh.kafka.handler.ConsumersGroupInstancesInstanceRecordsGetHandler;
import ca.sunlife.eadp.eventhub.mesh.kafka.streams.MessageReplayStreams;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class StreamsFactory {

    private StreamsFactory(){}

    public static ActiveConsumerMessageHandle createActiveConsumerMessageHandle() {
        return new ActiveConsumerMessageHandle();
    }

    public static ActiveConsumerStreamsAppMessageHandle createActiveConsumerStreamsAppMessageHandle() {
        return new ActiveConsumerStreamsAppMessageHandle();
    }

    public static ConsumersGroupInstancesInstanceRecordsGetHandler createConsumersGroupInstancesInstanceRecordsGetHandler() {
        return new ConsumersGroupInstancesInstanceRecordsGetHandler();
    }

    public static MessageReplayStreams createMessageReplayStreams() {
        return new MessageReplayStreams();
    }

    public static SubscribeTopic createSubscribeTopic(String group) {
        return new SubscribeTopic(group);
    }

    public static KafkaStreams createKafkaStreams(Topology topology, Properties props) { return new KafkaStreams(topology, props); }
}