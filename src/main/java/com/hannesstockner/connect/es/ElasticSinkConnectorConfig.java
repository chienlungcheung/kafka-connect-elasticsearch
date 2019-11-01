package com.hannesstockner.connect.es;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;


public class ElasticSinkConnectorConfig extends AbstractConfig {

  public static final String TYPE_NAME = "type.name";
  public static final String ELASTIC_CLUSTER_NAME = "es.cluster.name";
  public static final String ELASTIC_HOST = "es.host";
  public static final String ELASTIC_PORT = "es.port";
  private static final String TYPE_NAME_DOC = "Type of the Elastic index you want to work on.";
  private static final String ELASTIC_CLUSTER_NAME_DOC = "Elastic cluster name.";
  private static final String ELASTIC_HOST_DOC = "Elastic node host to connect.";
  private static final String ELASTIC_PORT_DOC = "Elastic transport port to connect.";
  public static final String INDEX_PREFIX = "index.prefix";
  private static final String INDEX_PREFIX_DOC = "Elastic index prefix.";
  public static final String TOPICS = "topics";
  private static final String TOPICS_DOC = "kafka topics.";

  public ElasticSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public ElasticSinkConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
      .define(TYPE_NAME, Type.STRING, Importance.HIGH, TYPE_NAME_DOC)
      .define(ELASTIC_CLUSTER_NAME, Type.STRING, Importance.HIGH, ELASTIC_CLUSTER_NAME_DOC)
      .define(INDEX_PREFIX, Type.STRING, Importance.HIGH, INDEX_PREFIX_DOC)
      .define(ELASTIC_PORT, Type.INT, Importance.HIGH, ELASTIC_PORT_DOC)
      .define(ELASTIC_HOST, Type.STRING, Importance.HIGH, ELASTIC_HOST_DOC)
      .define(TOPICS, Type.STRING, Importance.HIGH, TOPICS_DOC);
  }

  public String getIndexName() {
    return this.getString(INDEX_PREFIX);
  }

  public String getTypeName() {
    return this.getString(TYPE_NAME);
  }

  public String getElasticClusterName() {
    return this.getString(ELASTIC_CLUSTER_NAME);
  }

  public String getElasticHost() {
    return this.getString(ELASTIC_HOST);
  }

  public Integer getElasticPort() {
    return this.getInt(ELASTIC_PORT);
  }

}
