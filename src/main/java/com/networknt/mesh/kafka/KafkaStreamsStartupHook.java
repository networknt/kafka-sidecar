package com.networknt.mesh.kafka;

import com.networknt.server.Server;
import com.networknt.server.StartupHookProvider;
import com.networknt.utility.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamsStartupHook implements StartupHookProvider {
    private static final Logger logger= LoggerFactory.getLogger(KafkaStreamsStartupHook.class);
    public static GenericLightStreams genericLightStreams = new GenericLightStreams();

    @Override
    public void onStartup() {
        logger.info("KafkaStreamsStartupHook Starting !!! ");

        int port = Server.getServerConfig().getHttpsPort();
        String ip = NetUtils.getLocalAddressByDatagram();
        logger.info("ip = {} port = {}",ip,  port);
        // start the kafka stream process
        genericLightStreams.start(ip, port);
        logger.info("KafkaStreamsStartupHook onStartup ends.");
    }

}
