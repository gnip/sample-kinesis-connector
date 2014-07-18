package com.twitter.kinesis.connection;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.twitter.kinesis.utils.CredentialsProviderChain;
import com.twitter.kinesis.utils.Environment;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.log4j.Logger;

@Singleton
public class GnipKinesisClient extends AmazonKinesisClient {
    private static final Logger logger = Logger.getLogger(GnipKinesisClient.class);
    private Environment environment;

    @Inject
    public GnipKinesisClient(Environment environment,
                             CredentialsProviderChain providerChain) {
        super(providerChain);
        this.environment = environment;
    }

    public boolean configure() {
        String streamName = environment.kinesisStreamName();

        ListStreamsResult streamList = listStreams();
        if (streamList.getStreamNames().contains(streamName) && streamIsActive(streamName)) {
            logger.info(String.format("Found a kinesis stream in this account matching \"%s\".", streamName));
        } else {
            CreateStreamRequest streamRequest = new CreateStreamRequest()
                    .withStreamName(streamName)
                    .withShardCount(environment.shardCount());

            logger.info(String.format("Attempting to create kinesis stream \"%s\" with %d shards",
                    streamName,
                    streamRequest.getShardCount()));

            createStream(streamRequest);

            logger.info("Waiting for stream to become active...");
            boolean active = false;
            while (!active) {
                if (streamIsActive(streamName)) {
                    active = true;
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignore) {
                    }
                }
            }
            logger.info("Stream is active.");
        }
        return true;
    }

    private boolean streamIsActive(String streamName) {
        DescribeStreamResult streamResult = describeStream(streamName);
        if (streamResult.getStreamDescription().getStreamStatus().equals("ACTIVE")) {
            return true;
        } else {
            return false;
        }
    }
}
