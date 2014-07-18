package com.twitter.kinesis.perftest;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Executors;

public class TestKinesisAsyncClient {


  public static void main(String[] args) throws Exception {
    exec (1000, 10);
    exec (1000, 20);
    exec (1000, 30);
    exec (1000, 40);
    exec (1000, 50);
  }

  public static void exec(int msgCount, int poolSize) throws Exception {
    Random rnd = new Random();
    byte[] msg = new byte[2048];
    rnd.nextBytes(msg);
    ResultHandler handler = new ResultHandler(msgCount);

    AmazonKinesisAsyncClient client = new AmazonKinesisAsyncClient(Configure.getAwsCredentials(), Executors.newFixedThreadPool(poolSize));
    System.out.print("AWSCredentials: " + Configure.getAwsCredentials());
    double start = System.currentTimeMillis();
    for (int i = 0; i < msgCount; i++) {
      PutRecordRequest putRecordRequest = new PutRecordRequest();
      putRecordRequest.setStreamName("9shard");
      putRecordRequest.setData(ByteBuffer.wrap(msg));
      putRecordRequest.setPartitionKey("" + i);
      client.putRecordAsync(putRecordRequest, handler);
    }
    handler.await();
    double elapsed = (System.currentTimeMillis() - start) / 1000.0;
    System.out.printf("Time elapsed %02.1f (s), %02.3f msgs/sec, Error Count %d\r\n Msgs: %d, PoolSize %d", elapsed, (double) (msgCount) / (elapsed), handler.getErrorCount(),msgCount, poolSize);

  }

}
