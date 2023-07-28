package com.networknt.mesh.kafka.util;

import com.networknt.mesh.kafka.ActiveConsumerStartupHook;
import com.networknt.config.Config;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.entity.CreateConsumerInstanceRequest;
import com.networknt.utility.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class CreateConsumerGroup {

    private static final Logger logger= LoggerFactory.getLogger(CreateConsumerGroup.class);
    private final KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    private static final String KEY_FORMAT = "keyFormat";
    private static final String VALUE_FORMAT = "valueFormat";
    private String instanceId;
    private String listenerGroupName;

    public CreateConsumerGroup(String listenerGroupName) {
        this.listenerGroupName = listenerGroupName;
    }

    public String createConsumer() {
        try{

            Map<String, Object> map = new HashMap<>();
            if (map.get(KEY_FORMAT) == null) {
                map.put(KEY_FORMAT, config.getKeyFormat());
            }
            if (map.get(VALUE_FORMAT) == null) {
                map.put(VALUE_FORMAT, config.getValueFormat());
            }
            CreateConsumerInstanceRequest request = Config.getInstance().getMapper().convertValue(map, CreateConsumerInstanceRequest.class);
            if(logger.isDebugEnabled()) logger.debug("group = {} request = {} config = {}", listenerGroupName, request, Config.getInstance().getMapper().writeValueAsString(config));
            this.instanceId = ActiveConsumerStartupHook.kafkaConsumerManager.createConsumer(listenerGroupName, request.toConsumerInstanceConfig());

            if(!StringUtils.isEmpty(instanceId)){
                logger.debug("Created the consumer group {} , instance id is {}" , listenerGroupName , instanceId );
            }
        }catch(Exception e){
            logger.error("Exception while creating consumer group " , e);
        }

        return this.instanceId;
    }

    /**
     * 
     * @return instance Id Utility
     */
    public String getInstanceId(){
        return StringUtils.isEmpty(instanceId) ? null : instanceId;
    }

    /**
     * 
     * @param instanceId A string instance Id
     */
    public void setInstanceId(String instanceId){
        this.instanceId=instanceId;

    }
}
