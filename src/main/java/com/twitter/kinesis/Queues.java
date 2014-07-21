package com.twitter.kinesis;

import com.twitter.kinesis.utils.Environment;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Queues {

    public interface MessageConsumerQueue extends BlockingQueue<String> {
    }

    public interface BytesQueue extends BlockingQueue<byte[]> {
    }

    public static class MessageConsumerQueueImpl extends LinkedBlockingQueue<String> implements MessageConsumerQueue {
        public MessageConsumerQueueImpl(Environment environment) {
            super (environment.getMessageQueueSize());
        }
    }
}
