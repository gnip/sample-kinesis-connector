package com.twitter.kinesis.stream;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.kinesis.utils.Environment;
import com.twitter.kinesis.utils.KinesisStreamBuilder;
import com.twitter.kinesis.metrics.ShardMetric;
import com.twitter.kinesis.metrics.SimpleMetric;
import com.twitter.kinesis.metrics.SimpleMetricManager;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public class KinesisProducer implements Runnable {
  private final AmazonKinesisClient kinesisClient;
  private final String kinesisStreamName;
  private final Random rnd = new Random();
  private final Logger logger = Logger.getLogger(KinesisProducer.class);
  private final BlockingQueue<String> upstream;
  private final int kinesisShardCount;
  private final ScheduledExecutorService executor;
  private ShardMetric shardMetric;
  private SimpleMetric avgPutTime;
  private SimpleMetric batchSize;
  private SimpleMetric successCount;
  private SimpleMetric droppedMessageCount;

  public KinesisProducer(
          BlockingQueue<String> upstream,
          Environment environment,
          SimpleMetricManager metrics,
          ShardMetric shardMetric,
          AmazonKinesisClient client) {
    this.upstream = upstream;
    this.shardMetric = shardMetric;
    this.kinesisStreamName = environment.kinesisStreamName();
    this.kinesisShardCount = environment.shardCount();
    avgPutTime =  metrics.newSimpleMetric("Average Time to Write to Kinesis(ms)");
    batchSize = metrics.newSimpleMetric("Average Message Size to Kinesis (bytes)");
    successCount = metrics.newSimpleCountMetric("Successful writes to Kinesis");
    droppedMessageCount = metrics.newSimpleCountMetric("Failed writes to Kinesis");
    kinesisClient = client;

    ThreadFactory rateTrackerThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("kinesis-producer-thread-%d")
            .build();

    this.executor = Executors.newScheduledThreadPool(environment.getProducerThreadCount(), rateTrackerThreadFactory);
    buildStream();
  }

  public void start() {

    executor.execute(this);
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
      logger.warn("Error sending message, retrying", e);
      submitPutRequest(putRecordRequest, 500 * retryCount, retryCount +1);
    }
  }

  public void onSuccess(PutRecordRequest putRecordRequest, PutRecordResult putRecordResult) {
    batchSize.mark( putRecordRequest.getData().array().length);
    successCount.mark(1);
    shardMetric.track(putRecordResult);
  }

  private void buildStream() {
    KinesisStreamBuilder builder = new KinesisStreamBuilder();
    builder.shardCount(kinesisShardCount)
            .kinesisClient(this.kinesisClient)
            .streamName(this.kinesisStreamName)
            .build();
  }

  @Override
  public void run() {
    while (!Thread.interrupted()) {
      try {
        final String message = upstream.take();
        executor.submit(new Runnable() {
          @Override
          public void run() {
            sendMessage(message.getBytes());
          }
        });
      } catch (InterruptedException e) {
        logger.warn("Thread Interrupted");
      }
    }
  }
}
