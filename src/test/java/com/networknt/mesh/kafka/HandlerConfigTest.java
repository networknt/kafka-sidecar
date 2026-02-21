package com.networknt.mesh.kafka;

import com.networknt.handler.config.HandlerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class HandlerConfigTest {
    @Test
    public void testHandlerConfigLoad() {
        HandlerConfig handlerConfig = HandlerConfig.load();
        assertNotNull(handlerConfig);
        Assertions.assertEquals(handlerConfig.getHandlers().size(), 47);
        Assertions.assertEquals(handlerConfig.getChains().size(), 3);
        Assertions.assertEquals(handlerConfig.getPaths().size(), 36);
    }

}
