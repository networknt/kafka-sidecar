package com.networknt.mesh.kafka;

import com.networknt.handler.config.HandlerConfig;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class HandlerConfigTest {
    @Test
    public void testHandlerConfigLoad() {
        HandlerConfig handlerConfig = HandlerConfig.load();
        assertNotNull(handlerConfig);
        Assert.assertEquals(handlerConfig.getHandlers().size(), 47);
        Assert.assertEquals(handlerConfig.getChains().size(), 3);
        Assert.assertEquals(handlerConfig.getPaths().size(), 36);
    }

}
