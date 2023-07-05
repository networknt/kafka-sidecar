package ca.sunlife.eadp.eventhub.mesh.kafka.streams;

import ca.sunlife.eadp.eventhub.mesh.kafka.ActiveConsumerStartupHook;
import ca.sunlife.eadp.eventhub.mesh.kafka.ProducerStartupHook;
import ca.sunlife.eadp.eventhub.mesh.kafka.handler.ConsumersGroupInstancesInstanceRecordsGetHandler;
import ca.sunlife.eadp.eventhub.mesh.kafka.util.*;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.entity.AuditRecord;
import com.networknt.kafka.entity.ConsumerSeekRequest;
import com.networknt.kafka.entity.TopicReplayMetadata;
import com.networknt.kafka.producer.NativeLightProducer;
import com.networknt.kafka.producer.SidecarProducer;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.utility.ObjectUtils;
import com.networknt.utility.StringUtils;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

public class MessageReplayProcessor implements Processor<String, TopicReplayMetadata, String, TopicReplayMetadata> {

    private static final Logger logger= LoggerFactory.getLogger(MessageReplayProcessor.class);
    static final KafkaConsumerConfig consumerConfig= (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    private ProcessorContext processorContext;
    private SubscribeTopic subscribeTopic;
    private ActiveConsumerMessageHandle activeConsumerMessageHandle;
    private ActiveConsumerStreamsAppMessageHandle activeConsumerStreamsAppMessageHandle;
    public SidecarProducer lightProducer;


    @Override
    public void init(ProcessorContext<String, TopicReplayMetadata> context) {
        this.processorContext= context;
        activeConsumerMessageHandle= StreamsFactory.createActiveConsumerMessageHandle();
        activeConsumerStreamsAppMessageHandle = StreamsFactory.createActiveConsumerStreamsAppMessageHandle();
        if (ProducerStartupHook.producer != null) {
            lightProducer = (SidecarProducer) SingletonServiceFactory.getBean(NativeLightProducer.class);
        } else {
            logger.error("ProducerStartupHook is not configured and it is needed if DLQ is enabled");
            throw new RuntimeException("ProducerStartupHook is not loaded!");
        }
    }

    @Override
    public void process(Record<String, TopicReplayMetadata> record) {
        TopicReplayMetadata topicReplayMetadata = JsonMapper.objectMapper.convertValue(record.value(), TopicReplayMetadata.class);

        boolean processingRequired=false;

        /**
         * Further processing is required only if incoming topic is part of this consumer instance
         */
        if(topicReplayMetadata.isDlqIndicator() &&
                consumerConfig.getTopic().contains(topicReplayMetadata.getTopicName().split(consumerConfig.getDeadLetterTopicExt())[0])){
            processingRequired = true;
            logger.debug("Consumer configured with topic {} , replay attempt message received for topic {}, decision is to continue", consumerConfig.getTopic(), topicReplayMetadata.getTopicName());
            }
        else if(consumerConfig.getTopic().contains(topicReplayMetadata.getTopicName())){
            processingRequired = true;
            logger.debug("Consumer configured with topic {} , replay attempt message received for topic {}, decision is to continue", consumerConfig.getTopic(), topicReplayMetadata.getTopicName());
        }
        else{
            logger.debug("Consumer configured with topic {} , replay attempt message received for topic {}, decision is to return", consumerConfig.getTopic(), topicReplayMetadata.getTopicName());
            return;
        }

        if(processingRequired){
            Optional<RecordMetadata> recordMetadataOptional = processorContext.recordMetadata();
            AuditRecordUtil.publishAuditRecord(processorContext, record, recordMetadataOptional.get(), "", AuditRecord.AuditType.STREAMING_CONSUMER, AuditRecord.AuditStatus.SUCCESS);

            if(ObjectUtils.isEmpty(subscribeTopic) || StringUtils.isEmpty(subscribeTopic.getInstanceId())
                || ObjectUtils.isEmpty(ActiveConsumerStartupHook.kafkaConsumerManager)
                || ObjectUtils.isEmpty(ActiveConsumerStartupHook.kafkaConsumerManager.getExistingConsumerInstance(topicReplayMetadata.getConsumerGroup(),subscribeTopic.getInstanceId()))){
                subscribeTopic= StreamsFactory.createSubscribeTopic(topicReplayMetadata.getConsumerGroup());
                boolean subscriptionSuccess=subscribeTopic.subscribeToTopic(topicReplayMetadata);
                if(subscriptionSuccess){
                    try {

                        /**
                         * Seek to a particular offset
                         */

                        ConsumerSeekRequest.PartitionOffset partitionOffset= new ConsumerSeekRequest.PartitionOffset(topicReplayMetadata.getTopicName(), topicReplayMetadata.getPartition(), topicReplayMetadata.getStartOffset(), null);
                        ConsumerSeekRequest consumerSeekRequest = new ConsumerSeekRequest(Arrays.asList(partitionOffset), new ArrayList<>());
                        try {
                            ActiveConsumerStartupHook.kafkaConsumerManager.seek(topicReplayMetadata.getConsumerGroup(), subscribeTopic.getInstanceId(), consumerSeekRequest);
                            logger.info("Seeking to offset {} is successful ", topicReplayMetadata.getStartOffset());
                            ActiveConsumerMessageHandle.firstBatch=true;
                            ActiveConsumerStreamsAppMessageHandle.firstBatch=true;
                        }
                        catch (Exception e) {
                            throw new RuntimeException("Seeking to offset failed for topic "+ topicReplayMetadata.getStartOffset());
                        }


                        /**
                         * Read record only after seek is successful
                         */
                        long currentOffset=topicReplayMetadata.getStartOffset();
                        int counter=0;
                        while(currentOffset < topicReplayMetadata.getEndOffset() && counter < 3) {
                            String result =null;
                            try {
                                ConsumersGroupInstancesInstanceRecordsGetHandler readRecord = StreamsFactory.createConsumersGroupInstancesInstanceRecordsGetHandler();
                                result = readRecord.activeReadRecordUtil(topicReplayMetadata.getConsumerGroup(), subscribeTopic.getInstanceId(), null, null);
                                logger.debug("The Read Records are {}", result);
                            }
                            catch(Exception e){
                                throw new RuntimeException("Read records exception");
                            }
                            /**
                             * In case of no message return , we just receive [] in response , hence comparing with 2
                             */
                            if (!StringUtils.isEmpty(result) && result.length() > 2) {
                                if(topicReplayMetadata.isStreamingApp()){
                                    currentOffset = activeConsumerStreamsAppMessageHandle.listenOnMessage(lightProducer,result, currentOffset, topicReplayMetadata,
                                            ActiveConsumerStartupHook.kafkaConsumerManager, subscribeTopic.getInstanceId());
                                }else {
                                    currentOffset = activeConsumerMessageHandle.listenOnMessage(result, currentOffset, topicReplayMetadata,
                                            ActiveConsumerStartupHook.kafkaConsumerManager, subscribeTopic.getInstanceId(), lightProducer);
                                }
                                counter=0;
                            } else {
                                counter++;
                                logger.info("No message was retrieved from topic. Response code is {} , reattempt " , result);
                            }
                        }
                            /**
                             * After successful processing, invoke clean up, so that we create a new consumer with new topic next time
                             */

                            ActiveConsumerCleanup.cleanUp( ActiveConsumerStartupHook.kafkaConsumerManager.getExistingConsumerInstance(topicReplayMetadata.getConsumerGroup(),subscribeTopic.getInstanceId()),
                                    topicReplayMetadata.getConsumerGroup(), subscribeTopic.getInstanceId());

                        }

                    catch(Exception e){
                        logger.error("Exception occurred while reading record and processing batch ::: ",e);
                        AuditRecordUtil.publishAuditRecord(processorContext, record, recordMetadataOptional.get(), e.getMessage(), AuditRecord.AuditType.STREAMING_CONSUMER, AuditRecord.AuditStatus.FAILURE);
                        /**
                         * Clean up for before returning
                         */
                        ActiveConsumerCleanup.cleanUp( ActiveConsumerStartupHook.kafkaConsumerManager.getExistingConsumerInstance(topicReplayMetadata.getConsumerGroup(),subscribeTopic.getInstanceId()),
                                topicReplayMetadata.getConsumerGroup(), subscribeTopic.getInstanceId());
                    }
                }
                else{
                    logger.error("Can not subscribe to topic, reattempt. In case of reattempt error, check the payload or further restart the pod");
                }
            }

        }




    }

    @Override
    public void close() {

    }

}
