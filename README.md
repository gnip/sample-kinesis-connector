## sample-kinesis-connector

A sample application that consumes from Twitter enterprise streams using HBC and produces messages into Amazon Kinesis

## Requirements
* Java 1.7
* Maven

## Getting Started
This is an example app that takes in a Gnip Power Track and streams it into [AWS Kinesis](http://aws.amazon.com/kinesis/). This application can be deployed as is with a few edits to the configuration file:

  1. Clone the project: ```git clone https://github.com/twitterdev/sample-kinesis-connector sample-kinesis-connector && cd ~/sample-kinesis-connector```
  2. Create a config.properties file in src/main/resources: ```cd src/main/resources && mv config.properties.example config.properties```
  3. Edit the newly created file to contain your information: ```vim config.properteis```
  </br>
  Should look similar to this:
  ```bash
    gnip.user.name=YOUR_GNIP_USERNAME
    gnip.user.password=YOUR_GNIP_PASSWORD
    gnip.account.name=YOUR_GNIP_ACCOUNT_NAME
    gnip.product=YOUR_GNIP_PRODUCT
    gnip.stream.label=YOUR_GNIP_STREAM_LABEL

    aws.access.key=YOUR_AWS_ACCESS_LEY
    aws.secret.key=YOUR_AWS_SECRET_ACCEES_KEY
    aws.kinesis.stream.name=YOUR_DESIRED_KINESIS_STREAM_LABEL

    #Application configuration parameters-
    ########################
    #Do not change these settings
    ########################
    producer.thread.count=95
    batch.size=0
    aws.kinesis.shard.count=2
    message.queue.size=300
    bytes.queue.size=3000
    gnip.client.id=1
    rate.limit=-1
    metric.report.interval.seconds=60
  ```
  4. Build the project with Maven: ```cd ~/sample-kinesis-connector && mvn clean install```
  5. Run the project with: ```java -jar target/connector-<version_no>-jar-with-dependencies.jar```

## Notes
This sample has been tested on [Digital Ocean](https://www.digitalocean.com/) box with 1 Gb RAM. The ability for the application to produce to Kinesis is very sensitive to the quality the network connection. In testing, we were able to get a Gnip Decahose to flow quite well, only hitting the Kinesis rate limit a few times, all of which were recoverable errors.
