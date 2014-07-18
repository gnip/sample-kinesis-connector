package com.twitter.kinesis.perftest;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncRequest implements  Runnable {

  private ResultHandler handler;
  private byte[] message;
  private String shardName;
  AmazonKinesisClient client;
  AtomicBoolean done;
  Random rnd;

  public AsyncRequest(ResultHandler handler, byte[] message, String shardName) {
    this.handler = handler;
    this.message = message;
    this.shardName = shardName;
    client = new AmazonKinesisClient(Configure.getAwsCredentials());
    done = new AtomicBoolean(false);
    rnd = new Random();
  }

  @Override
  public void run() {
    while (!done.get()) {
      PutRecordRequest putRecordRequest = new PutRecordRequest();
      putRecordRequest.setStreamName(shardName);
      putRecordRequest.setData(ByteBuffer.wrap(message));
      putRecordRequest.setPartitionKey("" + rnd.nextInt() );

      try {
        PutRecordResult putRecordResult = client.putRecord(putRecordRequest);
        handler.onSuccess(putRecordRequest, putRecordResult);
      } catch (Throwable t) {
        t.printStackTrace();
        handler.onError((Exception)t);
      }
    }
  }

  public void done() {
    done.set(true);
  }

}
