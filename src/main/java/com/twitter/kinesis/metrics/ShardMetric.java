package com.twitter.kinesis.metrics;

import com.amazonaws.services.kinesis.model.PutRecordResult;

public interface ShardMetric extends SimpleMetric {
    void track (PutRecordResult result);
}
