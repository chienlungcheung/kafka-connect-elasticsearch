package com.hannesstockner.connect.es;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchSinkConnector extends SinkConnector {

  private static Logger logger = LoggerFactory.getLogger(ElasticsearchSinkConnector.class);

  private Map<String, String> configProperties;

  public static final String TYPE_NAME = "kafka";
  public static final String ES_CLUSTER_NAME = "gnome-adx";
  public static final String ES_HOST = "es.host";
  public static final String ES_PORT = "es.port";
  public static final String INDEX_PREFIX = "index.prefix";

  private String esHost;
  private String esPort;
  private String indexPrefix;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    configProperties = props;
    new ElasticSinkConnectorConfig(props);
    logger.info("connector[{}] started", this.getClass());
  }

  @Override
  public Class<? extends Task> taskClass() {
    return ElasticsearchSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    Map<String, String> config = new HashMap<>(configProperties);
    for (int i = 0; i < maxTasks; i++) {
      configs.add(config);
    }
    return configs;
  }

  @Override
  public void stop() {
    logger.info("connector[{}] stopped", this.getClass());
  }

  @Override
  public ConfigDef config() {
    logger.info("connector[{}] get config", this.getClass());
    return ElasticSinkConnectorConfig.conf();
  }
}
