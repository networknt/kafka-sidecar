package ca.sunlife.eadp.eventhub.mesh.kafka.util;

import ca.sunlife.eadp.eventhub.mesh.kafka.ActiveConsumerStartupHook;
import com.networknt.kafka.entity.ConsumerAssignmentRequest;
import com.networknt.kafka.entity.ConsumerSubscriptionRecord;
import com.networknt.kafka.entity.TopicPartition;
import com.networknt.kafka.entity.TopicReplayMetadata;
import com.networknt.utility.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class SubscribeTopic {

    private static final Logger logger= LoggerFactory.getLogger(SubscribeTopic.class);

    private String listenerGroupName;
    private String instanceId;

    public SubscribeTopic(String listenerGroupName) {
        this.listenerGroupName=listenerGroupName;
    }
    public boolean subscribeToTopic( TopicReplayMetadata topicReplayMetadata) {

        boolean subscribeTopicSuccess = false;

        try{

            CreateConsumerGroup createConsumerGroup= new CreateConsumerGroup(listenerGroupName );
            this.instanceId=createConsumerGroup.createConsumer();

            TopicPartition topicPartition = new TopicPartition(topicReplayMetadata.getTopicName(), topicReplayMetadata.getPartition());
            ConsumerAssignmentRequest consumerAssignmentRequest = new ConsumerAssignmentRequest(Arrays.asList(topicPartition));
            ActiveConsumerStartupHook.kafkaConsumerManager.assign(topicReplayMetadata.getConsumerGroup(), this.instanceId, consumerAssignmentRequest);
            logger.info("Subscribing to the topic {} successful", topicReplayMetadata.getTopicName() );


            subscribeTopicSuccess=true;



        }catch (Exception e){
            logger.error(e.getMessage(),e);
        }

        return subscribeTopicSuccess;
    }

    public String getInstanceId(){
        return StringUtils.isEmpty(this.instanceId) ? null : this.instanceId;
    }


}
