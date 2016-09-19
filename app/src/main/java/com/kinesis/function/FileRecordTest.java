package com.kinesis.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.util.StringUtils;

import org.junit.Test;

import java.util.Arrays;

public class FileRecordTest {

    @Test
    public void testFileRecord() {
        String streamName = "stream";
        String data = "some data";
        byte[] bytes = data.getBytes(StringUtils.UTF8);
        String str = FileRecordParser.asString(streamName, bytes);

        FileRecordParser frp = new FileRecordParser();
        frp.parse(str);
        assertEquals("stream name", streamName, frp.streamName);
        assertTrue("data bytes", Arrays.equals(bytes, frp.bytes));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidLineNoData() {
        String line = "line_without_data";
        FileRecordParser frp = new FileRecordParser();
        frp.parse(line);
    }
}
