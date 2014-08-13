package com.twitter.kinesis.metrics;

public class SimpleAverageMetric implements SimpleMetric {
    private long count;
    private long sample;
    private String name;

    SimpleAverageMetric(String name){
        this.name = name;
    }

    @Override
    public synchronized void mark(long size) {
        count++;
        sample += size;
    }

    public synchronized long getCount() {
        return count;
    }

    public synchronized double getAvg() {
        return count == 0 ? 0 : (double) sample / (double) count;
    }

    @Override
    public synchronized void reset() {
        count = 0;
        sample = 0;
    }

    public String toString() {
        return String.format("%s : %,.3f", name, this.getAvg());
    }

    public String getName() {
        return name;
    }
}
