package com.twitter.kinesis.utils;

public interface  ManagedRunnable extends  Runnable {
    void setPoolMonitor (PoolMonitor monitor);
}
