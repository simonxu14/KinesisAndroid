package com.kinesis.function;

import static org.junit.Assert.assertEquals;

import com.amazonaws.ClientConfiguration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;

@RunWith(RobolectricTestRunner.class)
@Config(manifest = Config.NONE)
public class KinesisRecorderConfigTest {

    @Test
    public void ClientConfigConstructor() {
        ClientConfiguration cConfig = new ClientConfiguration();
        cConfig.withConnectionTimeout(0);
        cConfig.withMaxConnections(5000);

        KinesisRecorderConfig kConfig = new KinesisRecorderConfig(cConfig);
        assertEquals(kConfig.getClientConfiguration().getConnectionTimeout(), 0);
        assertEquals(kConfig.getClientConfiguration().getMaxConnections(), 5000);
    }

    @Test
    public void defaultConstructorUsesDefaultClientConfiguration() {
        KinesisRecorderConfig kConfig = new KinesisRecorderConfig();
        ClientConfiguration cConfig = new ClientConfiguration();

        assertEquals(kConfig.getClientConfiguration().getConnectionTimeout(),
                cConfig.getConnectionTimeout());
        assertEquals(kConfig.getClientConfiguration().getMaxConnections(),
                cConfig.getMaxConnections());
        assertEquals(kConfig.getClientConfiguration().getMaxErrorRetry(),
                cConfig.getMaxErrorRetry());
        assertEquals(kConfig.getClientConfiguration().getSocketTimeout(),
                cConfig.getSocketTimeout());
    }

    @Test
    public void copyConstructor() {
        KinesisRecorderConfig kConfig = new KinesisRecorderConfig();
        kConfig.withMaxStorageSize(5);

        KinesisRecorderConfig copiedConfig = new KinesisRecorderConfig(kConfig);

        assertEquals(kConfig.getMaxStorageSize(),
                copiedConfig.getMaxStorageSize());

    }

    @Test
    public void testSetters() {

        KinesisRecorderConfig kConfig = new KinesisRecorderConfig();
        kConfig.withMaxStorageSize(100);
        assertEquals(kConfig.getMaxStorageSize(), 100);
    }

}
