package com.hbase.haxwell;

import com.google.common.base.Strings;
import com.hbase.haxwell.api.HaxwellSubscription;
import com.hbase.haxwell.config.HaxwellConfig;
import com.hbase.haxwell.config.HaxwellConfig.TopicMode;
import com.hbase.haxwell.util.ZookeeperHelper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * Haxwell Kafka Relay.
 * Configuration Parameters (Set as environment variables)
 * HAXWELL_ZOOKEEPER_CONNECTION_STRING
 * HAXWELL_ZOOKEEPER_CONNECTION_TIMEOUT
 * HAXWELL_SUBSCRIPTION_NAME
 * HAXWELL_HBASE_ZNODE
 * HAXWELL_KAFKA_BROKERS
 * HAXWELL_TOPIC_MODE (TOPIC_PER_TABLE/SINGLE_TOPIC)
 * HAXWELL_TOPIC_NAME
 * HAXWELL_TABLE_NAME
 */
@Log4j
public class HaxwellKafka {

  private static HaxwellConsumer haxwellConsumer;

  public static void main(String[] args) throws Exception {
    String zkConnectionString = System.getenv("HAXWELL_ZOOKEEPER_CONNECTION_STRING");
    if(Strings.isNullOrEmpty(zkConnectionString)) {
      zkConnectionString = "localhost";
    }
    int zkSessionTimeout = 20000;
    if(!Strings.isNullOrEmpty(System.getenv("HAXWELL_ZOOKEEPER_CONNECTION_TIMEOUT"))) {
      zkSessionTimeout = Integer.parseInt(System.getenv("HAXWELL_ZOOKEEPER_CONNECTION_TIMEOUT"));
    }
    log.info("Zookeeper Connection String: " +zkConnectionString +" Session Timeout: " +zkSessionTimeout);
    ZookeeperHelper zk = new ZookeeperHelper(zkConnectionString, zkSessionTimeout);

    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", zkConnectionString);
    if(!Strings.isNullOrEmpty(System.getenv("HAXWELL_HBASE_ZNODE"))) {
      conf.set("zookeeper.znode.parent", System.getenv("HAXWELL_HBASE_ZNODE"));
    }
    HaxwellSubscription haxwellSubscription = new HaxwellSubscriptionImpl(zk, conf);
    final String subscriptionName = System.getenv("HAXWELL_SUBSCRIPTION_NAME");

    if (!haxwellSubscription.hasSubscription(subscriptionName)) {
      haxwellSubscription.addSubscriptionSilent(subscriptionName);
    }
    HaxwellConfig haxwellConfig = new HaxwellConfig();
    haxwellConfig.setBrokerList(System.getenv("HAXWELL_KAFKA_BROKERS"));
    if(!Strings.isNullOrEmpty(System.getenv("HAXWELL_TOPIC_MODE"))) {
      haxwellConfig.setTopicMode(TopicMode.valueOf(System.getenv("HAXWELL_TOPIC_MODE")));
    } else {
      haxwellConfig.setTopicMode(TopicMode.TOPIC_PER_TABLE);
    }
    if(!Strings.isNullOrEmpty(System.getenv("HAXWELL_TOPIC_NAME"))) {
      haxwellConfig.setTopicName(System.getenv("HAXWELL_TOPIC_NAME"));
    }
    if(!Strings.isNullOrEmpty(System.getenv("HAXWELL_TABLE_NAME"))) {
      haxwellConfig.setTableName(System.getenv("HAXWELL_TABLE_NAME"));
    } else {
      haxwellConfig.setTableName("*");
    }

    KafkaEventListener kafkaEventListener = KafkaEventListener.builder()
          .haxwellConfig(haxwellConfig)
        .build();
    haxwellConsumer = new HaxwellConsumer(subscriptionName, 0, kafkaEventListener,
        System.getenv("HOST"), zk, conf);
    haxwellConsumer.start();

    HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
    server.createContext("/status", new StatusHandler());
    server.setExecutor(Executors.newSingleThreadExecutor());
    server.start();
  }

  static class StatusHandler implements HttpHandler {
    public void handle(HttpExchange t) throws IOException {
      t.sendResponseHeaders(200, 0);
      t.close();
    }
  }


}
