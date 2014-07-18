package com.twitter.kinesis.metrics;


import com.twitter.kinesis.Queues;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class QueueMetric implements SimpleMetric {
    private Queues.MessageConsumerQueue messageConsumerQueue;
    private Queues.BytesQueue bytesQueue;

    @Inject
    public QueueMetric(Queues.MessageConsumerQueue messageConsumerQueue, Queues.BytesQueue bytesQueue) {
        this.messageConsumerQueue = messageConsumerQueue;
        this.bytesQueue = bytesQueue;
    }

    @Override
    public String toString() {
        return String.format ("MessageConsumerQueue Size: %d\nMessageProducerQueue Size: %d", messageConsumerQueue.size(), bytesQueue.size());
    }

    @Override
    public void mark(long size) {
        //noop
    }

    @Override
    public void reset() {
        //noop
    }

    @Override
    public String getName() {
        return "QueueSize";
    }


}
