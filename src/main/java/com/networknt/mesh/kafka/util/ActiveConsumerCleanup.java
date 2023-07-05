package ca.sunlife.eadp.eventhub.mesh.kafka.util;

import ca.sunlife.eadp.eventhub.mesh.kafka.ActiveConsumerStartupHook;
import com.networknt.kafka.consumer.KafkaConsumerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveConsumerCleanup {
    private static final Logger logger= LoggerFactory.getLogger(ActiveConsumerCleanup.class);
    public static void cleanUp(KafkaConsumerState<?, ?, ?, ?> consumerInstance, String group, String instance){

        try {
            /**
             * After successful processing, unsubscribe the topic, so that we don't consume unnecessary messages
             */

            try {
                ActiveConsumerStartupHook.kafkaConsumerManager.unsubscribe(group, instance);
                logger.debug("The unsubscription completed ");
            }

            catch(Exception e) {
                throw new RuntimeException("Unsubscription error");
            }

            /**
             * After successful processing, delete consumer instance, so that we create a new one with new topic next time
             */

            try {
                ActiveConsumerStartupHook.kafkaConsumerManager.deleteConsumer(group, instance);
                logger.debug("The unsubscription completed ");
            }

            catch(Exception e) {
                throw new RuntimeException("Delete consumer instance error");
            }

        }
        catch(Exception e){
            logger.error("Consumer clean up failed , next replay attempt may fail. Restart pod before next replay attempt ", e);
        }
    }
}
