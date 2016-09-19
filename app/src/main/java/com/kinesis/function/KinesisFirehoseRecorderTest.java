
package com.kinesis.function;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.util.StringUtils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class KinesisFirehoseRecorderTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private KinesisFirehoseRecorder recorder;

    @Before
    public void setup() throws IOException {
        AWSCredentialsProvider provider = new StaticCredentialsProvider(
                new AnonymousAWSCredentials());
        recorder = new KinesisFirehoseRecorder(temp.newFolder(), Regions.US_WEST_2, provider);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullRecord() throws IOException {
        recorder.saveRecord((byte[]) null, "stream");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyData() throws IOException {
        byte[] data = new byte[0];
        recorder.saveRecord(data, "stream");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyStream() throws IOException {
        recorder.saveRecord("valid".getBytes(StringUtils.UTF8), "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullStream() throws IOException {
        recorder.saveRecord("valid".getBytes(StringUtils.UTF8), null);
    }

}
