package com.twitter.kinesis.metrics;

import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import java.io.PrintWriter;
import java.io.StringWriter;

public class ShardMetricLogging implements ShardMetric {

    private Multiset<String> shardCounter;

    public ShardMetricLogging() {
        shardCounter = ConcurrentHashMultiset.create();
    }

    @Override
    public void track(PutRecordResult result) {
        shardCounter.add(result.getShardId());
    }

    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.println ("Shard Distributuion: ");
        pw.println ("====================");
        for (String key : shardCounter.elementSet()) {
            pw.printf("\t%s : %d\n", key, shardCounter.count(key));
        }
        pw.close();
        return sw.toString();
    }

    @Override
    public void mark(long size) {
        //noop
    }

    @Override
    public void reset() {
        shardCounter.clear();
    }

    @Override
    public String getName() {
        return "Shard Distribution";
    }
}
