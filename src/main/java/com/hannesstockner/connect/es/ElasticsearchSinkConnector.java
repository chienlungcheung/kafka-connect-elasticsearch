package com.hannesstockner.connect.es;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ElasticsearchSinkConnector extends SinkConnector {

  private static Logger logger = LoggerFactory.getLogger(ElasticsearchSinkConnector.class);

  /**
   * common configs
   */
  public static final String NAME = "name";
  public static final String CONNECTOR_CLASS = "connector.class";
  public static final String TASKS_MAX = "tasks.max";
  public static final String TOPICS = "topics";

  /**
   * es-sink-connector dependent configs
   */
  public static final String ES_CLUSTER_NAME = "es.cluster.name";
  public static final String ES_SERVERS = "es.servers";
  public static final String INDEX_PREFIX = "index.prefix";
  public static final String TYPE_NAME = "type";


  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Unique name for the connector. Attempting to register again with the same name will fail.")
      .define(CONNECTOR_CLASS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The Java class for the connector.")
      .define(TASKS_MAX, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "The maximum number of tasks that should be created for this connector. The connector may create fewer tasks if it cannot achieve this level of parallelism.")
      .define(TOPICS, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "A comma-separated list of topics to use as input for this connector.")
      .define(ES_CLUSTER_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The name of es cluster to be connected.")
      .define(ES_SERVERS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The servers of the es cluster, [host:port] for each server, comma separated.")
//      .define(ES_PORT, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "The transport tcp port of an node of the es cluster.")
      .define(INDEX_PREFIX, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Index prefix to be written to the es cluster.")
      .define(TYPE_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Type to be written to the es cluster.");
  private List<String> topics;

  private Map<String, String> configProperties;

  @Override
  public String version() {
    return "1.0";
  }

  @Override
  public void start(Map<String, String> props) {
    configProperties = props;
    Monitor.start(0, 1, TimeUnit.SECONDS);
    logger.info("connector[{}] started, configuration is {}", this.getClass(), configProperties);
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
    Monitor.stop();
    logger.info("connector[{}] stopped", this.getClass());
  }

  @Override
  public ConfigDef config() {
    logger.info("connector[{}] get config-def", this.getClass());
    return CONFIG_DEF;
  }
}
