package com.twitter.kinesis;

import com.twitter.kinesis.metrics.QueueMetric;
import com.twitter.kinesis.metrics.SimpleMetric;
import com.twitter.kinesis.metrics.SimpleMetricManager;
import com.twitter.kinesis.utils.Environment;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Queues {

    public interface MessageConsumerQueue extends BlockingQueue<String> {
    }

    public interface BytesQueue extends BlockingQueue<byte[]> {
    }

    @Singleton
    public static class MessageConsumerQueueImpl extends LinkedBlockingQueue<String> implements MessageConsumerQueue {
        @Inject
        public MessageConsumerQueueImpl(Environment environment) {
            super (environment.getMessageQueueSize());
        }
    }

    @Singleton
    public static class MeteredMessageConsumerQueueImpl extends LinkedBlockingQueue<String> implements MessageConsumerQueue {
      private SimpleMetric inboundActivities;

      @Inject
      public MeteredMessageConsumerQueueImpl(Environment environment, SimpleMetricManager manager) {
        super(environment.getMessageQueueSize());
        this.inboundActivities = manager.newSimpleCountMetric("Gnip received activities");
      }

      @Override
      public boolean offer(String s, long timeout, TimeUnit unit) throws InterruptedException {
        this.inboundActivities.mark(1);
        return super.offer(s, timeout, unit);
      }

      @Override
      public boolean offer(String s) {
        this.inboundActivities.mark(1);
        return super.offer(s);
      }
    }

    @Singleton
    public static class BytesQueueImpl extends LinkedBlockingQueue<byte[]> implements BytesQueue{
        @Inject
        public BytesQueueImpl(Environment environment) {
            super(environment.getBytesQueueSize());
        }
    }

}
