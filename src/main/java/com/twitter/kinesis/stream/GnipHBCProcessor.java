package com.twitter.kinesis.stream;

import com.twitter.kinesis.Queues;
import com.twitter.kinesis.metrics.SimpleMetric;
import com.twitter.kinesis.utils.Environment;
import com.twitter.kinesis.utils.ManagedRunnable;
import com.twitter.kinesis.utils.PoolMonitor;
import com.google.inject.Inject;
import com.twitter.hbc.*;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.EnterpriseStreamingEndpoint;
import com.twitter.hbc.core.endpoint.RealTimeEnterpriseStreamingEndpoint;
import com.twitter.hbc.core.processor.LineStringProcessor;
import com.twitter.hbc.httpclient.auth.BasicAuth;

public class GnipHBCProcessor implements ManagedRunnable {
  private Queues.MessageConsumerQueue downstream;
  private SimpleMetric inboundActivityCount;
  private PoolMonitor monitor;
  private Client client;
  private Environment environment;

  @Inject
  public GnipHBCProcessor(Queues.MessageConsumerQueue downstream, Environment environment) {
    this.downstream = downstream;
    this.environment = environment;
    try {
      this.client = setupClient();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setPoolMonitor(PoolMonitor monitor) {
    this.monitor = monitor;
  }

  @Override
  public void run() {
    this.client.connect();
  }

  public Client setupClient() throws InterruptedException {
    Queues.MessageConsumerQueue queue = new Queues.MessageConsumerQueueImpl(environment);
    return new ClientBuilder()
            .name("PowerTrackClient-02")
            .hosts(Constants.ENTERPRISE_STREAM_HOST)
            .endpoint(endpoint())
            .authentication(auth())
            .processor(new LineStringProcessor(queue))
            .build();
  }

  private BasicAuth auth() {
    return new BasicAuth(this.environment.userName(), this.environment.userPassword());
  }

  private EnterpriseStreamingEndpoint endpoint() {
    String account = this.environment.accountName();
    String label = this.environment.streamLabel();
    String product = this.environment.product();
    return new RealTimeEnterpriseStreamingEndpoint(account, product, label);
  }
}
