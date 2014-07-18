package com.twitter.kinesis.utils;

import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Singleton;

import javax.inject.Inject;

@Singleton
public class GnipRateLimiter {

    RateLimiter rateLimiter;
    @Inject
    public GnipRateLimiter(Environment environment) {
        double rate = environment.getRateLimit();
        if (rate > 0 ) {
            rateLimiter = RateLimiter.create(rate);
        }
    }

    public synchronized void limit (int permits) {
        if (rateLimiter != null) {
            rateLimiter.acquire(permits);
        }
    }
}
