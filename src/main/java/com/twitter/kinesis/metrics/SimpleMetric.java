package com.twitter.kinesis.metrics;

public interface SimpleMetric {
    void mark(long size);

    void reset();

    String getName();
}
