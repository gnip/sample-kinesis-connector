package com.twitter.kinesis.utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.inject.Singleton;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.TreeSet;

@Singleton
public class Environment implements AWSCredentialsProvider {
  private static final Logger logger = Logger.getLogger(Environment.class);
  private static Properties props;

  public void configure() {
    try {
      logger.info("loading properties from classpath");
      InputStream properties = Environment.class.getClassLoader().getResourceAsStream("config.properties");
      props = new Properties();
      props.load(properties);
      logProperties();
    } catch (IOException e) {
      logger.error("Could not load properties, streams cannot be configured");
      throw new RuntimeException("Could not load properties");
    }
  }

  public void logProperties() {
    TreeSet<String> keys = new TreeSet<>(props.stringPropertyNames());

    for (String key : keys) {
      logger.info(key + ": " + props.get(key));
    }
  }

  public String userName() {
    return props.getProperty("gnip.user.name");
  }

  public String userPassword() {
    return props.getProperty("gnip.user.password");
  }

  public String streamLabel() {
    return props.getProperty("gnip.stream.label");
  }

  public String accountName() {
    return props.getProperty("gnip.account.name");
  }

  public String product() {
    return props.getProperty("gnip.product");
  }

  public String clientId() {
    return props.getProperty("gnip.client.id");
  }

  public String publisher() {
    return props.getProperty("gnip.publisher", "twitter");
  }

  public int batchSize() {
    return Integer.parseInt(props.getProperty("batch.size", "0"));
  }

  public int getProducerThreadCount() {
    return Integer.parseInt(props.getProperty("thread.count", "30"));
  }

  public int getMessageQueueSize() {
    return Integer.parseInt(props.getProperty("message.queue.size", "300"));
  }

  public int getBytesQueueSize() {
    return Integer.parseInt(props.getProperty("bytes.queue.size", "300"));
  }

  public double getRateLimit() {
    return Double.parseDouble(props.getProperty("rate.limit", "-1"));
  }

  public int getReportInterval() {
    return Integer.parseInt(props.getProperty("metric.report.interval.seconds", "60"));
  }

  public String kinesisStreamName() {
    return props.getProperty("aws.kinesis.stream.name");
  }

  public int shardCount() {
    return Integer.parseInt(props.getProperty("aws.kinesis.shard.count"));
  }

  @Override
  public AWSCredentials getCredentials() {
    AWSCredentials credentials = new AWSCredentials() {
      @Override
      public String getAWSAccessKeyId() {
        String value = props.getProperty("aws.access.key");
        return value;
      }

      @Override
      public String getAWSSecretKey() {
        String value = props.getProperty("aws.secret.key");
        return value;
      }
    };
    return credentials;
  }

  @Override
  public void refresh() {
    // No-op
  }
}
