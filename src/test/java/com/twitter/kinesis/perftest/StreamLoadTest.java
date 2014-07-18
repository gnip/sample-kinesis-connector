package com.twitter.kinesis.perftest;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

    public class StreamLoadTest {
    static AmazonKinesisClient client
            = new AmazonKinesisClient(Configure.getAwsCredentials());

    public static void main(String[] args) {

        TestKinesisSyncClient.printHeader();
        for (int i = 1; i < 10; i++) {
            String streamName = "" + i + "_shard";
            createStream(streamName, i);
            for ( int j = 10; j < 101; j += 10) {
                TestKinesisSyncClient.exec (10,j, streamName);
            }
            deleteStream(streamName);
        }
    }

    private static void deleteStream(String streamName) {
        try {
            client.deleteStream(streamName);
            long startTime = System.currentTimeMillis();
            long endTime = startTime + (10 * 60 * 1000);
            while (System.currentTimeMillis() < endTime) {
                try {
                    client.describeStream(streamName);
                    Thread.sleep(1000);
                } catch (ResourceNotFoundException e) {
                    return;
                } catch (InterruptedException e) {

                }
            }
        } catch (ResourceNotFoundException e) {
            return;
        }
        throw new RuntimeException("Couldn't delete " + streamName);
    }


    private static void createStream(String streamName, int shardCount) {
        client.createStream(streamName, shardCount);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(20 * 1000);
            } catch (Exception e) {
            }

            try {

                DescribeStreamResult describeStreamResponse = client.describeStream(streamName);
                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                if (streamStatus.equals("ACTIVE")) {
                    break;
                }
                //
                // sleep for one second
                //
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                }
            } catch (ResourceNotFoundException e) {
            }
        }
        if (System.currentTimeMillis() >= endTime) {
            throw new RuntimeException("Stream " + streamName + " never went active");
        }
    }

}
