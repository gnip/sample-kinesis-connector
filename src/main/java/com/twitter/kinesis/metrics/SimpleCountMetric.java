package com.twitter.kinesis.metrics;

public class SimpleCountMetric extends SimpleAverageMetric {
    SimpleCountMetric(String name) {
        super(name);
    }

    @Override
    public String toString() {
        return String.format("%s : %d", this.getName(), this.getCount());
    }
}
