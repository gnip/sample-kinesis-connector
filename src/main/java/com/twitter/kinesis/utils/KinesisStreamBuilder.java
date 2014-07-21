package com.twitter.kinesis.utils;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsResult;

import java.util.logging.Logger;

public class KinesisStreamBuilder {

  private AmazonKinesisClient client;
  private String name;
  private int shardCount;
  private final Logger logger = Logger.getLogger(KinesisStreamBuilder.class.toString());

  public KinesisStreamBuilder() {
    new AmazonKinesisClient();
  }

  public KinesisStreamBuilder kinesisClient(AmazonKinesisClient client) {
    this.client = client;
    return this;
  }

  public KinesisStreamBuilder streamName(String name) {
    this.name = name;
    return this;
  }

  public KinesisStreamBuilder shardCount(int shardCount) {
    this.shardCount = shardCount;
    return this;
  }

  public void build() {
    ListStreamsResult streamList = client.listStreams();
    if (streamList.getStreamNames().contains(name) && streamIsActive(name)) {
      logger.info(String.format("Found a kinesis stream in this account matching \"%s\".", name));
    } else {
      CreateStreamRequest streamRequest = new CreateStreamRequest()
              .withStreamName(name)
              .withShardCount(this.shardCount);
      logger.info(String.format("Attempting to create kinesis stream \"%s\" with %d shards",
              name,
              streamRequest.getShardCount()));
      client.createStream(streamRequest);
      logger.info("Waiting for stream to become active...");
      boolean active = false;

      while (!active) {
        if (streamIsActive(name)) {
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
  }

  private boolean streamIsActive(String streamName) {
    DescribeStreamResult streamResult = client.describeStream(streamName);
    return streamResult.getStreamDescription().getStreamStatus().equals("ACTIVE");
  }
}
