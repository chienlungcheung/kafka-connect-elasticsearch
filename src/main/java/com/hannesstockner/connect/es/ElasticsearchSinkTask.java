package com.hannesstockner.connect.es;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

public class ElasticsearchSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);
  private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
  private String typeName;
  private String indexPrefix;
  private Client client;

  private final ExecutorService writer = Executors.newSingleThreadExecutor();

  private static class WriteTask implements Runnable {
    private static BlockingDeque<BulkRequestBuilder> requests = new LinkedBlockingDeque<>(1000);
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        BulkRequestBuilder bulkRequestBuilder = null;
        try {
          bulkRequestBuilder = requests.take();
        } catch (InterruptedException e) {
          return;
        }
        int reqCnt = bulkRequestBuilder.numberOfActions();
        BulkResponse bulkResponse = null;
        try {
          bulkResponse = bulkRequestBuilder.execute().actionGet(60, TimeUnit.SECONDS);
          if (bulkResponse.hasFailures()) {
            for (BulkItemResponse item : bulkResponse.getItems()) {
              if (item.isFailed()) {
                Monitor.getFailedRequests().incrementAndGet();
              } else {
                Monitor.getSuccessfulRequests().incrementAndGet();
              }
              log.warn("send to es failed: failure = {}", item.getFailure());
            }
          } else {
            Monitor.getSuccessfulRequests().addAndGet(reqCnt);
          }
        } catch (Exception e) {
          log.warn("send to es failed: exception = ", e);
        }
        Monitor.getToSentRequests().addAndGet(-reqCnt);
      }
    }
  }
  @Override
  public void start(Map<String, String> props) {
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

      client
        .admin()
        .indices()
        .preparePutTemplate("kafka_template")
        .setPatterns(Collections.singletonList(indexPrefix + "*"))
        .addMapping(typeName, new HashMap<String, Object>() {{
          put("date_detection", false);
          put("numeric_detection", false);
        }})
        .get();

    } catch (Exception e) {
      log.error("Couldn't connect to es: ", e);
      System.exit(-1);
    }

    writer.submit(new WriteTask());
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      log.debug("no records to be sent.");
      return;
    }
    String indexSuffix = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now());
    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
    for (SinkRecord record : records) {
      log.debug("Processing record type = {}, record content = {}",
        record.value().getClass(),
        record);
      IndexRequest indexRequest = client.prepareIndex(indexPrefix + record.topic() + "-" + indexSuffix, typeName)
        .setSource(gson.toJson(record), XContentType.JSON)
        .request();
      bulkRequestBuilder.add(indexRequest);
    }
    try {
      WriteTask.requests.offerFirst(bulkRequestBuilder, 500, TimeUnit.MICROSECONDS);
      Monitor.getToSentRequests().addAndGet(bulkRequestBuilder.numberOfActions());
    } catch (InterruptedException e) {
      log.warn("queue of WriteTask is full, may be sending message to es is too slow. exception: ", e);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
  }

  @Override
  public void stop() {
    if (client != null) {
      writer.shutdown();
      client.close();
    }
  }

  @Override
  public String version() {
    return new ElasticsearchSinkConnector().version();
  }
}
