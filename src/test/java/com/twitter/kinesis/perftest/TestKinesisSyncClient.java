package com.twitter.kinesis.perftest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestKinesisSyncClient {

    public static void main(String[] args) throws Exception {
        System.out.println ("Starting");
        printHeader();
        exec (10, 10, "5shard");
    }

    public static void exec (int duration,  int threadCount, String shardName) {
        final byte[] message = new byte[2048];
        final Random rnd = new Random();
        rnd.nextBytes(message);
        List<AsyncRequest> list = new ArrayList<>();

        ResultHandler handler = new ResultHandler(duration);
        for (int i =0; i < threadCount; i ++ ) {
            list.add (new AsyncRequest(handler,message, shardName));
        }

        handler.start();
        long start = System.currentTimeMillis();
        for (Runnable r : list) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.start();
        }
        try {
            handler.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (AsyncRequest request : list) {
            request.done();
        }


        int msgCount = handler.getReceivedCount();
        double elapsed = (double)(System.currentTimeMillis() - start) / 1000.0;

        System.out.printf ( "%s\t\t%d\t\t\t\t%d\t\t\t\t%02.3f\t\t\t%02.3f\n",
                shardName,
                msgCount,
                threadCount,
                elapsed,
                (elapsed * threadCount)/(double)msgCount
//                handler.getErrorCount()
        );
    }

    public static void printHeader() {
        System.out.println ("#of Shards\t#of messages\t#of threads\t\tduration\ttime per message");
    }
}
