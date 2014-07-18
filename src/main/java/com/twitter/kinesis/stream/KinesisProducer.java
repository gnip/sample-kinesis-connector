package com.twitter.kinesis.stream;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.twitter.kinesis.utils.Environment;
import com.twitter.kinesis.Queues;
import com.twitter.kinesis.metrics.ShardMetric;
import com.twitter.kinesis.metrics.SimpleMetric;
import com.twitter.kinesis.metrics.SimpleMetricManager;
import com.twitter.kinesis.utils.GnipRateLimiter;
import com.twitter.kinesis.utils.ManagedRunnable;
import com.twitter.kinesis.utils.PoolMonitor;
import com.google.inject.Inject;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Random;

public class KinesisProducer implements ManagedRunnable {
    private final AmazonKinesisClient kinesisClient;
    private final String kinesisStreamName;
    private final Random rnd = new Random();
    private final Logger logger = Logger.getLogger(KinesisProducer.class);
    private final Queues.BytesQueue upstream;
    private ShardMetric shardMetric;
    private GnipRateLimiter limiter;
    private PoolMonitor monitor;
    private SimpleMetric avgPutTime;
    private SimpleMetric batchSize;
    private SimpleMetric successCount;
    private SimpleMetric droppedMessageCount;


    @Inject
    public KinesisProducer(
            Queues.BytesQueue upstream,
            Environment environment,
            SimpleMetricManager metrics,
            ShardMetric shardMetric,
            AmazonKinesisClient client,
            GnipRateLimiter limiter) {
        this.upstream = upstream;
        this.shardMetric = shardMetric;
        this.limiter = limiter;
        this.kinesisStreamName = environment.kinesisStreamName();
        avgPutTime =  metrics.newSimpleMetric("Average Time to Write to Kinesis(ms)");
        batchSize = metrics.newSimpleMetric("Average Message Size to Kinesis (bytes)");
        successCount = metrics.newSimpleCountMetric("Successful writes to Kinesis");
        droppedMessageCount = metrics.newSimpleCountMetric("Failed writes to Kinesis");

        kinesisClient = client;
    }

    @Override
    public void setPoolMonitor(PoolMonitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public void run () {
        monitor.threadStarted();
        try {
        while (!Thread.interrupted() && !monitor.isPoolStopped().get()) {
            try {
                byte[] message = upstream.take();
                sendMessage(message);
            } catch (InterruptedException e) {
                logger.warn ("Thread Interrupted");
            }
        }
        } finally {
            monitor.threadStopped();
        }
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
        limiter.limit(1);
        shardMetric.track(putRecordResult);
    }
}
