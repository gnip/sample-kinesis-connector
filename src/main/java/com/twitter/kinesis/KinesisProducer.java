package com.twitter.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.kinesis.metrics.ShardMetric;
import com.twitter.kinesis.metrics.SimpleMetric;
import com.twitter.kinesis.metrics.SimpleMetricManager;
import com.twitter.kinesis.utils.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class KinesisProducer implements Runnable {
  private final AmazonKinesisClient kinesisClient;
  private final String kinesisStreamName;
  private final Random rnd = new Random();
  private final Logger logger = LoggerFactory.getLogger(Environment.class);
  private final BlockingQueue<String> upstream;
  private ShardMetric shardMetric;
  private SimpleMetric avgPutTime;
  private SimpleMetric batchSize;
  private SimpleMetric successCount;
  private SimpleMetric droppedMessageCount;

  public KinesisProducer(
          BlockingQueue<String> upstream,
          Environment environment,
          SimpleMetricManager metrics,
          AmazonKinesisClient client,
          String kinesisStreamName,
          ShardMetric shardMetric) {
    this.upstream = upstream;
    this.kinesisStreamName = kinesisStreamName;
    this.shardMetric = shardMetric;
    avgPutTime = metrics.newSimpleMetric("Average Time to Write to Kinesis(ms)");
    batchSize = metrics.newSimpleMetric("Average Message Size to Kinesis (bytes)");
    successCount = metrics.newSimpleCountMetric("Successful writes to Kinesis");
    droppedMessageCount = metrics.newSimpleCountMetric("Failed writes to Kinesis");
    kinesisClient = client;
  }

  private void sendMessage(final byte[] bytes) {
    final PutRecordRequest putRecordRequest = new PutRecordRequest();
    putRecordRequest.setStreamName(kinesisStreamName);
    putRecordRequest.setData(ByteBuffer.wrap(bytes));
    putRecordRequest.setPartitionKey("" + rnd.nextInt());
    submitPutRequest(putRecordRequest, 0);
  }

  private void submitPutRequest(final PutRecordRequest putRecordRequest, int sleepTime) {
    submitPutRequest(putRecordRequest, sleepTime, 1);
  }

  private void submitPutRequest(final PutRecordRequest putRecordRequest, int sleepTime, int retryCount) {
    long start = System.currentTimeMillis();
    try {
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      PutRecordResult putRecordResult =
              kinesisClient.putRecord(putRecordRequest);
      avgPutTime.mark(System.currentTimeMillis() - start);
      onSuccess(putRecordRequest, putRecordResult);
    } catch (ResourceNotFoundException notFound) {
      logger.error("Skipping put of request due to resource not found exception: " + notFound.getMessage() + "\nWe were likely unable to provision a stream");
    } catch (Exception t) {
      onError(putRecordRequest, retryCount, t);
    }
  }

  public void onError(final PutRecordRequest putRecordRequest, final int retryCount, Exception e) {
    if (retryCount > 3) {
      logger.error("Failed retry 3 times... dropping message.", e);
      droppedMessageCount.mark(1);
    } else {
      logger.warn("Error sending message, retrying");
      submitPutRequest(putRecordRequest, 500 * retryCount, retryCount + 1);
    }
  }

  public void onSuccess(PutRecordRequest putRecordRequest, PutRecordResult putRecordResult) {
    batchSize.mark(putRecordRequest.getData().array().length);
    successCount.mark(1);
    shardMetric.track(putRecordResult);
  }

  @Override
  public void run() {
    while (!Thread.interrupted()) {
      try {
        String message = upstream.take();
        sendMessage(message.getBytes());
      } catch (InterruptedException e) {
        logger.warn("Thread Interrupted");
      }
    }
  }

  public static class Builder {

    private final Random rnd = new Random();
    private final Logger logger = LoggerFactory.getLogger(Environment.class);
    private BlockingQueue<String> upstream;
    private AmazonKinesisClient kinesisClient;
    private SimpleMetricManager metricManager;
    private ShardMetric shardMetric;
    private String kinesisStreamName;
    private Environment environment;
    private int shardCount;

    public Builder kinesisClient(AmazonKinesisClient client) {
      this.kinesisClient = client;
      return this;
    }

    public Builder streamName(String name) {
      this.kinesisStreamName = name;
      return this;
    }

    public Builder shardCount(int shardCount) {
      this.shardCount = shardCount;
      return this;
    }

    public Builder upstream(BlockingQueue<String> upstream) {
      this.upstream = upstream;
      return this;
    }

    public Builder environment(Environment environment) {
      this.environment = environment;
      return this;
    }

    public Builder simpleMetricManager(SimpleMetricManager metricManager) {
      this.metricManager = metricManager;
      return this;
    }

    public Builder shardMetric(ShardMetric shardMetric) {
      this.shardMetric = shardMetric;
      return this;
    }

    public void buildAndStart() {
      // TODO: tolerate null attributes
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

      ThreadFactory threadFactory = new ThreadFactoryBuilder()
              .setNameFormat("kinesis-producer-thread-%d")
              .build();

      for (int i = 0; i < environment.getProducerThreadCount(); i++) {
        KinesisProducer producer = new KinesisProducer(
                this.upstream,
                this.environment,
                this.metricManager,
                this.kinesisClient,
                this.kinesisStreamName,
                this.shardMetric
        );
        Executors.newSingleThreadExecutor(threadFactory).execute(producer);
      }

    }

    private boolean streamIsActive(String streamName) {
      DescribeStreamResult streamResult = this.kinesisClient.describeStream(streamName);
      return streamResult.getStreamDescription().getStreamStatus().equals("ACTIVE");
    }
  }
}
