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
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);
  private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
  private String typeName;
  private String indexPrefix;
  private Client client;

  @Override
  public void start(Map<String, String> props) {
    AbstractConfig parsedConfig = new AbstractConfig(ElasticsearchSinkConnector.CONFIG_DEF, props);
    typeName = parsedConfig.getString(ElasticsearchSinkConnector.TYPE_NAME);
    final String clusterName = parsedConfig.getString(ElasticsearchSinkConnector.ES_CLUSTER_NAME);
    final String esHost = parsedConfig.getString(ElasticsearchSinkConnector.ES_HOST);
    final int esPort = parsedConfig.getInt(ElasticsearchSinkConnector.ES_PORT);
    indexPrefix = parsedConfig.getString(ElasticsearchSinkConnector.INDEX_PREFIX);

    try {
      // 避免重启报错：java.lang.IllegalStateException: availableProcessors is already set to [32], rejecting [32]
      System.setProperty("es.set.netty.runtime.available.processors", "false");
      Settings settings = Settings.builder().put("cluster.name", clusterName).build();
      client = new PreBuiltTransportClient(settings)
        .addTransportAddress(new TransportAddress(InetAddress.getByName(esHost), esPort));

      client
        .admin()
        .indices()
        .preparePutTemplate("kafka_template")
        .setTemplate(indexPrefix + "*")
        .addMapping(typeName, new HashMap<String, Object>() {{
          put("date_detection", false);
          put("numeric_detection", false);
        }})
        .get();

    } catch (Exception e) {
      log.error("Couldn't connect to es: ", e);
      System.exit(-1);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      log.debug("no records to be sent.");
      return;
    }
    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
    for (SinkRecord record : records) {
      log.debug("Processing record type = {}, record content = {}",
        record.value().getClass(),
        record);
      IndexRequest indexRequest = client.prepareIndex(indexPrefix + record.topic(), typeName)
        .setSource(gson.toJson(record), XContentType.JSON)
        .request();
      bulkRequestBuilder.add(indexRequest);
    }
    BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      for (BulkItemResponse item : bulkResponse.getItems()) {
        log.warn("send to es failed: failure = {}", item.getFailure());
      }
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
  }

  @Override
  public void stop() {
    client.close();
  }

  @Override
  public String version() {
    return new ElasticsearchSinkConnector().version();
  }
}
