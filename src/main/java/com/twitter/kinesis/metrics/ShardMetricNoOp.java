package com.twitter.kinesis.metrics;

import com.amazonaws.services.kinesis.model.PutRecordResult;

public class ShardMetricNoOp implements ShardMetric{
    @Override
    public void track(PutRecordResult result) {
    }

    @Override
    public void mark(long size) {
    }

    @Override
    public void reset() {
    }

    @Override
    public String getName() {
        return "";
    }

}
