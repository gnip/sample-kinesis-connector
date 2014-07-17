package com.twitter.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.kinesis.utils.Environment;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class KinesisProducer implements Runnable {

  private final AmazonKinesisClient kinesisClient;
  private final String kinesisStreamName;
  private final Random rnd = new Random();
  private final Logger logger = Logger.getLogger(KinesisProducer.class);
  private final RateLimiter rateLimiter;
  private final ExecutorService executer;
  private BlockingQueue<String> upstream;

  public KinesisProducer(BlockingQueue<String> upstream, Environment environment) {
    this.kinesisStreamName = environment.kinesisStreamName();
    this.upstream = upstream;
    this.kinesisClient = new AmazonKinesisClient(environment);
    this.rateLimiter = RateLimiter.create(environment.getRateLimit());

    ThreadFactory rateTrackerThreadFactory = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("kinesis-producer-thread-%d")
            .build();

    executer = Executors.newScheduledThreadPool(environment.getProducerThreadCount(), rateTrackerThreadFactory);
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
    try {
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);
      onSuccess();
    } catch (Exception t) {
      onError(putRecordRequest, retryCount, t);
    }
  }

  public void onError(final PutRecordRequest putRecordRequest, final int retryCount, Exception e) {
    if (retryCount > 3) {
      logger.error("Failed retry 3 times... dropping message.", e);
    } else {
      logger.warn("Error sending message, retrying", e);
      submitPutRequest(putRecordRequest, 500 * retryCount, retryCount + 1);
    }
  }

  public void onSuccess() {
    rateLimiter.acquire();
  }

  public void start() {
    executer.execute(this);
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
}
