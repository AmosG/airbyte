package io.airbyte.integrations.source.databricks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;


public class DatabricksSourceConfig {

    /**
     * Currently only S3 is supported. So the data source config is always .
     */
    static final String DEFAULT_DATABRICKS_PORT = "443";
    static final String DEFAULT_DATABASE_SCHEMA = "public";
    static final boolean DEFAULT_PURGE_STAGING_DATA = true;

    private final String databricksServerHostname;
    private final String databricksHttpPath;
    private final String databricksPort;
    private final String databricksPersonalAccessToken;
    private final String databaseSchema;
    private final boolean purgeStagingData;

    public DatabricksSourceConfig(final String databricksServerHostname,
                                  final String databricksHttpPath,
                                  final String databricksPort,
                                  final String databricksPersonalAccessToken,
                                  final String databaseSchema,
                                  final boolean purgeStagingData) {
        this.databricksServerHostname = databricksServerHostname;
        this.databricksHttpPath = databricksHttpPath;
        this.databricksPort = databricksPort;
        this.databricksPersonalAccessToken = databricksPersonalAccessToken;
        this.databaseSchema = databaseSchema;
        this.purgeStagingData = purgeStagingData;
    }

    public static DatabricksSourceConfig get(final JsonNode config) {
//        Preconditions.checkArgument(
//                config.has("accept_terms") && config.get("accept_terms").asBoolean(),
//                "You must agree to the Databricks JDBC Terms & Conditions to use this connector");

        return new DatabricksSourceConfig(
                config.get("databricks_server_hostname").asText(),
                config.get("databricks_http_path").asText(),
                config.has("databricks_port") ? config.get("databricks_port").asText() : DEFAULT_DATABRICKS_PORT,
                config.get("databricks_personal_access_token").asText(),
                config.has("database_schema") ? config.get("database_schema").asText() : DEFAULT_DATABASE_SCHEMA,
                config.has("purge_staging_data") ? config.get("purge_staging_data").asBoolean() : DEFAULT_PURGE_STAGING_DATA);
    }


    public String getDatabricksServerHostname() {
        return databricksServerHostname;
    }

    public String getDatabricksHttpPath() {
        return databricksHttpPath;
    }

    public String getDatabricksPort() {
        return databricksPort;
    }

    public String getDatabricksPersonalAccessToken() {
        return databricksPersonalAccessToken;
    }

    public String getDatabaseSchema() {
        return databaseSchema;
    }

    public boolean isPurgeStagingData() {
        return purgeStagingData;
    }

}
