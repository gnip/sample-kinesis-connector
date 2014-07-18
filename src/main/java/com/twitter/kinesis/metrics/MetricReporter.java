package com.twitter.kinesis.metrics;

import com.twitter.kinesis.utils.Environment;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.Timer;
import java.util.TimerTask;

@Singleton
public class MetricReporter {
    private SimpleMetricManager metrics;

    private int reportInterval;
    @Inject
    public MetricReporter(final SimpleMetricManager metrics, Environment environment) {
        this.metrics = metrics;
        this.reportInterval = environment.getReportInterval()*1000;
    }

    public void start() {
        Timer t = new Timer(true);
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                metrics.report();
            }
        };
        t.schedule(task, 10 * 1000, reportInterval);
    }
}
