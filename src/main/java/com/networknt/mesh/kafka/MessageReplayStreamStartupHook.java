package ca.sunlife.eadp.eventhub.mesh.kafka;

import ca.sunlife.eadp.eventhub.mesh.kafka.streams.MessageReplayStreams;
import ca.sunlife.eadp.eventhub.mesh.kafka.util.StreamsFactory;
import com.networknt.server.Server;
import com.networknt.server.StartupHookProvider;
import com.networknt.utility.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageReplayStreamStartupHook implements StartupHookProvider {

    private static Logger logger= LoggerFactory.getLogger(MessageReplayStreamStartupHook.class);
    public static MessageReplayStreams messageReplayStreams = null;
    @Override
    public void onStartup() {

        logger.info("MessageReplayStreamStartupHook Starting !!! ");

        int port = Server.getServerConfig().getHttpsPort();
        String ip = NetUtils.getLocalAddressByDatagram();
        logger.info("ip = {} port = {}", ip, port);

        messageReplayStreams = StreamsFactory.createMessageReplayStreams();

        // start the kafka message replay stream process
        messageReplayStreams.start(ip, port);

        logger.info("MessageReplayStreamStartupHook onStartup ends.");
    }
}
