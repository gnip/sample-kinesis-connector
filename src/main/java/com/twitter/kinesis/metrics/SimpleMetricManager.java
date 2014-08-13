package com.twitter.kinesis.metrics;

import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@Singleton
public class SimpleMetricManager {

    Map<String, SimpleMetric> map = new HashMap<String, SimpleMetric>();
    Logger logger = LoggerFactory.getLogger(SimpleMetricManager.class);

    public synchronized void report() {
        StringBuilder buf = new StringBuilder();
        buf.append("\n=================\n");
        for (String key : map.keySet()) {
            SimpleMetric value = map.get(key);
            buf.append(value.toString());
            buf.append ("\n");
            value.reset();
        }
        logger.info(buf.toString());
    }

    public synchronized SimpleMetric newSimpleCountMetric(String s) {
        SimpleMetric metric = map.get(s);
        if ( metric == null ){
            metric = new SimpleCountMetric(s);
            map.put(s, metric);
        }
        return metric;
    }

    public synchronized SimpleMetric newSimpleMetric(String s) {
        SimpleMetric metric = map.get(s);
        if ( metric == null ){
            metric = new SimpleAverageMetric(s);
            map.put(s, metric);
        }
        return metric;
    }

    public synchronized void registerMetric (SimpleMetric metric) {
        map.put (metric.getName(), metric);
    }
}
