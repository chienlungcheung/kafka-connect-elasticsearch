package com.hannesstockner.connect.es;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ElasticsearchSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);
  private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
  private String typeName;
  private String indexPrefix;
  private Client client;
  private BulkProcessor bulkProcessor;
  @Override
  public void start(Map<String, String> props) {
    if (props == null) {
      log.error("props is null in {}.start()", this.getClass());
      return;
    }
    AbstractConfig parsedConfig = new AbstractConfig(ElasticsearchSinkConnector.CONFIG_DEF, props);
    typeName = parsedConfig.getString(ElasticsearchSinkConnector.TYPE_NAME);
    final String clusterName = parsedConfig.getString(ElasticsearchSinkConnector.ES_CLUSTER_NAME);
    final String esServers = parsedConfig.getString(ElasticsearchSinkConnector.ES_SERVERS);
    indexPrefix = parsedConfig.getString(ElasticsearchSinkConnector.INDEX_PREFIX);

    try {
      // 避免重启报错：java.lang.IllegalStateException: availableProcessors is already set to [32], rejecting [32]
      System.setProperty("es.set.netty.runtime.available.processors", "false");
      Settings settings = Settings
        .builder()
        .put("cluster.name", clusterName)
        .put("client.transport.sniff", true)
        .build();
      String[] hostPortPair = esServers.split(",");
      TransportAddress[] transportAddresses = new TransportAddress[hostPortPair.length];
      for (int i = 0; i < hostPortPair.length; ++i) {
        String pair = hostPortPair[i];
        String[] hostPort = pair.split(":");
        transportAddresses[i] = new TransportAddress(InetAddress.getByName(hostPort[0]), Integer.parseInt(hostPort[1]));
      }
      client = new PreBuiltTransportClient(settings)
        .addTransportAddresses(transportAddresses);
    } catch (Exception e) {
      log.error("Couldn't connect to es: ", e);
      System.exit(-1);
    }

    bulkProcessor = BulkProcessor.builder(client,
      new BulkProcessor.Listener() {
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {

        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
          Monitor.getToBeSentRequests().decrementAndGet();
          Monitor.getSuccessfulRequests().incrementAndGet();
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
          log.error("executionId = {}, bulk request failed: {}", executionId, failure.getMessage());
          if (Monitor.getToBeSentRequests() == null || Monitor.getFailedRequests() == null) {
            log.error("Monitor.getToBeSentRequests() == null || Monitor.getFailedRequests() == null");
          } else {
            Monitor.getToBeSentRequests().decrementAndGet();
            Monitor.getFailedRequests().incrementAndGet();
          }
        }
      })
      .setBulkActions(1000)
      .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
      .setFlushInterval(TimeValue.timeValueSeconds(5))
      .setConcurrentRequests(1)
      .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
      .build();
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      log.debug("no records to be sent.");
      return;
    }
    String indexSuffix = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now());
    for (SinkRecord record : records) {
      log.debug("Processing record type = {}, record content = {}",
        record.value().getClass(),
        record);
      IndexRequest indexRequest = client.prepareIndex(indexPrefix + record.topic() + "-" + indexSuffix, typeName)
        .setSource(gson.toJson(record), XContentType.JSON)
        .request();
      bulkProcessor.add(indexRequest);
      Monitor.getToBeSentRequests().incrementAndGet();
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    bulkProcessor.flush();
  }

  @Override
  public void stop() {
    if (client != null) {
      bulkProcessor.close();
      client.close();
    }
  }

  @Override
  public String version() {
    return new ElasticsearchSinkConnector().version();
  }
}
