/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.databricks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.db.jdbc.streaming.AdaptiveStreamingQueryConfig;
import io.airbyte.db.jdbc.streaming.NoOpStreamingQueryConfig;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.source.jdbc.AbstractJdbcSource;
import java.sql.JDBCType;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.airbyte.db.factory.DatabaseDriver;


public class DatabricksSource extends AbstractJdbcSource<JDBCType> implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksSource.class);

  public static final String DATABRICKS_USER_FIELD = "UID";
  public static final String DATABRICKS_USERNAME = "token";
  public static final String DATABRICKS_PASSWORD_FIELD = "PWD";
  public static final String DATABRICKS_DRIVER_CLASS = DatabaseDriver.DATABRICKS.getDriverClassName();

  // TODO insert your driver name. Ex: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  static final String DRIVER_CLASS = DATABRICKS_DRIVER_CLASS;

  public DatabricksSource() {
    // TODO: if the JDBC driver does not support custom fetch size, use NoOpStreamingQueryConfig
    // instead of AdaptiveStreamingQueryConfig.
    super(DRIVER_CLASS, NoOpStreamingQueryConfig::new, JdbcUtils.getDefaultSourceOperations());

  }


  // TODO The config is based on spec.json, update according to your DB
  @Override
  public JsonNode toDatabaseConfig(final JsonNode config) {

    final DatabricksSourceConfig databricksConfig = DatabricksSourceConfig.get(config);

    final StringBuilder jdbcUrl = new StringBuilder(getDatabricksConnectionString(databricksConfig));

    var result = Jsons.jsonNode(ImmutableMap.builder()
            .put(JdbcUtils.JDBC_URL_KEY, jdbcUrl.toString())
//            .put(DATABRICKS_USER_FIELD, DATABRICKS_USERNAME)
//            .put(DATABRICKS_PASSWORD_FIELD, databricksConfig.getDatabricksPersonalAccessToken())
            .build());

    LOGGER.info("myprint config: {}",String.valueOf(result));

    return result;
  }

  static String getDatabricksConnectionString(final DatabricksSourceConfig databricksConfig) {
    return String.format(DatabaseDriver.DATABRICKS.getUrlFormatString(),
            databricksConfig.getDatabricksServerHostname(),
            databricksConfig.getDatabricksHttpPath())+";UID=token;PWD=dapi0123456789abcdefghij0123456789AB";
  }

  @Override
  public Set<String> getExcludedInternalNameSpaces() {
    // TODO Add tables to exclude, Ex "INFORMATION_SCHEMA", "sys", "spt_fallback_db", etc
    return Set.of("");
  }

  public static void main(final String[] args) throws Exception {
    final Source source = new DatabricksSource();
    LOGGER.info("starting source: {}", DatabricksSource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("completed source: {}", DatabricksSource.class);
  }

}
