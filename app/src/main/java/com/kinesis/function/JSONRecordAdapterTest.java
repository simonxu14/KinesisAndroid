
package com.kinesis.function;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import android.util.Base64;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.util.StringUtils;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;

import java.nio.ByteBuffer;

@RunWith(RobolectricTestRunner.class)
@Config(manifest = Config.NONE)
public class JSONRecordAdapterTest {

    private final String stream = "TestStream";
    private final String partitionKey = "TestPartitionKey";
    private final String explicitHashKey = "TestHashKey";
    private final String sequenceNumberForOrdering = "1";
    private final ByteBuffer data = ByteBuffer.wrap("TestData".getBytes(StringUtils.UTF8));

    @Test
    public void convertRequestToJSONAllElementsIntact() {
        PutRecordRequest putRequest = new PutRecordRequest();

        putRequest.withStreamName(stream).withPartitionKey(partitionKey)
                .withExplicitHashKey(explicitHashKey).withData(data)
                .withSequenceNumberForOrdering(sequenceNumberForOrdering);

        JSONRecordAdapter adapter = new JSONRecordAdapter();
        JSONObject json = adapter.translateFromRecord(putRequest);

        try {
            assertTrue(json.getString(JSONRecordAdapter.STREAM_NAME_FIELD).equalsIgnoreCase(stream));
            assertTrue(json.getString(JSONRecordAdapter.DATA_FIELD_KEY).equalsIgnoreCase(
                    Base64.encodeToString("TestData".getBytes(StringUtils.UTF8), Base64.DEFAULT)));
            assertTrue(json.getString(JSONRecordAdapter.PARTITION_KEY_FIELD).equalsIgnoreCase(
                    partitionKey));
            assertTrue(json.getString(JSONRecordAdapter.SEQUENCE_NUMBER_FIELD).equalsIgnoreCase(
                    sequenceNumberForOrdering));
            assertTrue(json.getString(JSONRecordAdapter.EXPLICIT_HASH_FIELD).equalsIgnoreCase(
                    explicitHashKey));

        } catch (JSONException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void nullRequestReturnsNull() {
        JSONRecordAdapter adapter = new JSONRecordAdapter();
        assertNull(adapter.translateFromRecord(null));
    }

    @Test(expected = AmazonClientException.class)
    public void convertRequestWhenStreamNull() {
        PutRecordRequest putRequest = new PutRecordRequest();
        putRequest.withPartitionKey(partitionKey).withData(data);

        JSONRecordAdapter adapter = new JSONRecordAdapter();
        adapter.translateFromRecord(putRequest);
    }

    @Test(expected = AmazonClientException.class)
    public void convertRequestWhenStreamEmpty() {
        PutRecordRequest putRequest = new PutRecordRequest();
        String streamName = "";
        putRequest.withStreamName(streamName).withPartitionKey(partitionKey).withData(data);

        JSONRecordAdapter adapter = new JSONRecordAdapter();
        adapter.translateFromRecord(putRequest);
    }

    @Test(expected = AmazonClientException.class)
    public void convertRequestWhenDataNull() {
        PutRecordRequest putRequest = new PutRecordRequest();
        putRequest.withStreamName(stream).withPartitionKey(partitionKey);

        JSONRecordAdapter adapter = new JSONRecordAdapter();
        adapter.translateFromRecord(putRequest);
    }

    @Test(expected = AmazonClientException.class)
    public void convertRequestWhenPartitionKeyNull() {
        PutRecordRequest putRequest = new PutRecordRequest();
        putRequest.withStreamName(stream).withData(data);

        JSONRecordAdapter adapter = new JSONRecordAdapter();
        adapter.translateFromRecord(putRequest);
    }

    @Test(expected = AmazonClientException.class)
    public void convertRequestWhenPartitionKeyEmpty() {
        PutRecordRequest putRequest = new PutRecordRequest();
        String partitionKey = "";
        putRequest.withStreamName(stream).withData(data).withPartitionKey(partitionKey);

        JSONRecordAdapter adapter = new JSONRecordAdapter();
        adapter.translateFromRecord(putRequest);
    }

    @Test
    public void testStaticGetters() throws JSONException {

        PutRecordRequest putRequest = new PutRecordRequest();
        putRequest.withStreamName(stream).withPartitionKey(partitionKey).withData(data);

        JSONRecordAdapter adapter = new JSONRecordAdapter();
        JSONObject json = adapter.translateFromRecord(putRequest);

        assertArrayEquals(data.array(), JSONRecordAdapter.getData(json).array());
        assertTrue(partitionKey.equalsIgnoreCase(JSONRecordAdapter.getPartitionKey(json)));
        assertTrue(stream.equalsIgnoreCase(JSONRecordAdapter.getStreamName(json)));
    }
}
