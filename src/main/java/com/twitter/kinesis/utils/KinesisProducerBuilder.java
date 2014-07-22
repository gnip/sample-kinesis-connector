package com.twitter.kinesis.utils;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.twitter.kinesis.KinesisProducer;
import com.twitter.kinesis.metrics.ShardMetric;
import com.twitter.kinesis.metrics.SimpleMetricManager;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.BlockingQueue;

public class KinesisProducerBuilder {

  private final Random rnd = new Random();
  private final org.slf4j.Logger logger =  LoggerFactory.getLogger(Environment.class);

  private BlockingQueue<String> upstream;
  private AmazonKinesisClient kinesisClient;
  private SimpleMetricManager metricManager;
  private ShardMetric shardMetric;
  private String kinesisStreamName;
  private Environment environment;
  private int shardCount;

  public KinesisProducerBuilder kinesisClient(AmazonKinesisClient client) {
    this.kinesisClient = client;
    return this;
  }

  public KinesisProducerBuilder streamName(String name) {
    this.kinesisStreamName = name;
    return this;
  }

  public KinesisProducerBuilder shardCount(int shardCount) {
    this.shardCount = shardCount;
    return this;
  }

  public KinesisProducerBuilder upstream(BlockingQueue<String> upstream) {
    this.upstream = upstream;
    return this;
  }

  public KinesisProducerBuilder environment(Environment environment) {
    this.environment = environment;
    return this;
  }

  public KinesisProducerBuilder simpleMetricManager(SimpleMetricManager metricManager) {
    this.metricManager = metricManager;
    return this;
  }

  public KinesisProducerBuilder shardMetric(ShardMetric shardMetric) {
    this.shardMetric = shardMetric;
    return this;
  }

  public KinesisProducer build() {
    ListStreamsResult streamList = this.kinesisClient.listStreams();
    if (streamList.getStreamNames().contains(this.kinesisStreamName) && streamIsActive(this.kinesisStreamName)) {
      this.logger.info(String.format("Found a kinesis stream in this account matching \"%s\".", this.kinesisStreamName));
    } else {
      CreateStreamRequest streamRequest = new CreateStreamRequest()
              .withStreamName(this.kinesisStreamName)
              .withShardCount(this.shardCount);
      this.logger.info(String.format("Attempting to create kinesis stream \"%s\" with %d shards",
              this.kinesisStreamName,
              streamRequest.getShardCount()));
      this.kinesisClient.createStream(streamRequest);
      this.logger.info("Waiting for stream to become active...");
      boolean active = false;

      while (!active) {
        if (streamIsActive(this.kinesisStreamName)) {
          active = true;
        } else {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ignore) {
          }
        }
      }
      this.logger.info("Stream is active.");
    }
    KinesisProducer producer = new KinesisProducer(
            this.upstream,
            this.environment,
            this.metricManager,
            this.kinesisClient,
            this.kinesisStreamName,
            this.shardMetric
    );
    return producer;
  }

  private boolean streamIsActive(String streamName) {
    DescribeStreamResult streamResult = this.kinesisClient.describeStream(streamName);
    return streamResult.getStreamDescription().getStreamStatus().equals("ACTIVE");
  }
}
