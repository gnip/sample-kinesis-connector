package com.twitter.kinesis.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class PoolMonitor {
    AtomicBoolean isStopped = new AtomicBoolean(false);
    int threadCount=0;
    private String poolName;
    private List<EmptyPoolListener> emptyPoolListeners = new LinkedList<>();


    public PoolMonitor (String poolName) {
        this.poolName = poolName;
    }

    public synchronized void threadStarted() {
        threadCount ++;
    }

    public synchronized void threadStopped() {
        threadCount --;
        if ( threadCount <= 0 ) {
            notifyEmptyPoolListener();
        }
    }

    public void stopPool () {
        isStopped.set(true);
    }

    public AtomicBoolean isPoolStopped () {
        return isStopped;
    }

    public synchronized void addEmptyPoolListener (EmptyPoolListener emptyPoolListener) {
        emptyPoolListeners.add(emptyPoolListener);
    }

    private void notifyEmptyPoolListener() {
        for (EmptyPoolListener listener: emptyPoolListeners ){
            listener.poolEmpty (poolName);
        }
    }



}
