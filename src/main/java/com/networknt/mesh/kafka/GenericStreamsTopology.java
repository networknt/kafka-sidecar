package com.networknt.mesh.kafka;

import com.networknt.kafka.entity.StreamsDLQMetadata;
import org.apache.kafka.streams.Topology;

import java.util.Map;

public interface GenericStreamsTopology {
    Topology buildTopology();
    Map<String, StreamsDLQMetadata> getDlqTopicMetadataMap();
}
