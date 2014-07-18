package com.twitter.kinesis.utils;

import com.google.inject.Injector;
import org.apache.log4j.Logger;

public class ExecutorPool {
    private String name;
    private Class<? extends Runnable> runnable;
    private Injector injector;
    private int threadCount;
    private PoolMonitor monitor;
    Logger logger = Logger.getLogger(ExecutorPool.class);

    public ExecutorPool (String name, Class<? extends ManagedRunnable> runnable, Injector injector, int threadCount) {
        monitor = new PoolMonitor(name);
        this.name = name;
        this.runnable = runnable;
        this.injector = injector;
        this.threadCount = threadCount;
    }

    public void start () {
        logger.info ("Executor "  + name + "- Starting " + threadCount + " threads");
        for (int i = 0 ; i < threadCount; i ++) {
            ManagedRunnable runnable = (ManagedRunnable) injector.getInstance(this.runnable);
            runnable.setPoolMonitor(monitor);
            Thread thread = new Thread (runnable, name + "-" + i);
            thread.setDaemon(true);
            thread.start();
        }
    }

    public void addEmptyPoolListener (EmptyPoolListener emptyPoolListener) {
        monitor.addEmptyPoolListener(emptyPoolListener);
    }

    public void stopPool() {
        monitor.stopPool();
    }
}

