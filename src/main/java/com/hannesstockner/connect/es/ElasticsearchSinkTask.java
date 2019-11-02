package com.hannesstockner.connect.es;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
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
    for (SinkRecord record : records) {
      log.debug("Processing record type = {}, record content = {}",
        record.value().getClass(),
        record);
      boolean failed = true;
      while (failed) {
        try {
          client
            .prepareIndex(indexPrefix + record.topic(), typeName)
            .setSource(gson.toJson(record), XContentType.JSON)
            .get();
          failed = false;
        } catch (Exception e) {
          log.error("Couldn't send record to es, will try: ", e);
          try {
            log.info("send record to es, retrying...");
            Thread.sleep(5000);
          } catch (InterruptedException ignored) {
          }
        }
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
