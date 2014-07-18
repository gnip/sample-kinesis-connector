package com.twitter.kinesis.perftest;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.twitter.kinesis.metrics.ShardMetricLogging;

import java.util.Timer;
import java.util.TimerTask;

public class ResultHandler implements AsyncHandler<PutRecordRequest, PutRecordResult> {

  boolean done = false;
  int receivedCount;
  int errorCount ;

  ShardMetricLogging shardLog;
  Timer t;
  private int duration;

  public ResultHandler(int duration) {
    this.duration = duration;
    t = new Timer(true);
    shardLog = new ShardMetricLogging();
  }

  public void start() {
    TimerTask task = new TimerTask(){

      @Override
      public void run() {
        done();
      }
    };
    t.schedule(task,duration*1000);
  }

  private synchronized void done () {
    done =true;
    t.cancel();
    notify();
  }


  public synchronized void await () throws InterruptedException {
    if (!done) {
      wait();
    }
  }

  @Override
  public void onError(Exception e) {
    takeActionForException(e);
    incrementError();
  }

  @Override
  public synchronized void onSuccess(PutRecordRequest putRecordRequest, PutRecordResult putRecordResult) {
    increment();
    shardLog.track(putRecordResult);
  }

  public synchronized int getErrorCount() {
    return errorCount;
  }

  public synchronized void incrementError() {
    errorCount ++;
    increment();
    if (errorCount > 10) {
      System.out.println ("exiting due to errors");
      done();
    }
  }

  private void increment() {
    receivedCount ++;
    if (receivedCount % 1000 == 0) {
      System.out.println ("-received: " + receivedCount + " error: " + errorCount);
    }
  }

  public int getReceivedCount () {
    return receivedCount;
  }

  private void takeActionForException(Exception e) {
    if (e.getClass() == ResourceNotFoundException.class) {
      ResourceNotFoundException resourceNotFoundException = (ResourceNotFoundException)e;
      System.out.print("Exiting due to ResourceNotFoundException: " + resourceNotFoundException.getLocalizedMessage());
      System.exit(1);
    }
  }
}
