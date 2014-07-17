package com.twitter.kinesis;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.EnterpriseStreamingEndpoint;
import com.twitter.hbc.core.endpoint.RealTimeEnterpriseStreamingEndpoint;
import com.twitter.hbc.core.processor.LineStringProcessor;
import com.twitter.hbc.httpclient.auth.BasicAuth;
import com.twitter.kinesis.utils.Environment;

import java.util.concurrent.LinkedBlockingQueue;

public class ConnectorApplication {

  private Client client;
  private Environment environment;
  private KinesisProducer producer;

  public ConnectorApplication() {
    environment = new Environment();
  }

  public static void main(String[] args) {
    ConnectorApplication application = new ConnectorApplication();
    application.configure();
    application.start();
  }

  private void configure() {
    environment.configure();

    LinkedBlockingQueue<String> downstream = new LinkedBlockingQueue<>(10000);

    client = new ClientBuilder()
            .name("PowerTrackClient-02")
            .hosts(Constants.ENTERPRISE_STREAM_HOST)
            .endpoint(endpoint())
            .authentication(auth())
            .processor(new LineStringProcessor(downstream))
            .build();

    producer = new KinesisProducer(downstream, environment);
  }

  private BasicAuth auth() {
    return new BasicAuth(this.environment.userName(), this.environment.userPassword());
  }

  private void start() {
    client.connect();
    producer.start();
  }

  private EnterpriseStreamingEndpoint endpoint() {
    String account = this.environment.accountName();
    String label = this.environment.streamLabel();
    String product = this.environment.product();
    return new RealTimeEnterpriseStreamingEndpoint(account, product, label);
  }
}
