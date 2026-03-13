/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.sqlserver;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.mapping.ReadMapping;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SqlServerClient
        extends BaseJdbcClient
{
    private static final Joiner DOT_JOINER = Joiner.on(".");
    private final TypeManager typeManager;

    @Inject
    public SqlServerClient(JdbcConnectorId connectorId, BaseJdbcConfig config, TypeManager typeManager)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new SQLServerDriver(), config));
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "sp_rename %s, %s",
                    singleQuote(catalogName, oldTable.getSchemaName(), oldTable.getTableName()),
                    singleQuote(newTable.getTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "sp_rename %s, %s, 'COLUMN'",
                    singleQuote(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName(), jdbcColumn.getColumnName()),
                    singleQuote(newColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * Get views from SQL Server INFORMATION_SCHEMA.VIEWS system table.
     * This method retrieves view definitions for the specified schema and table names
     * and stores them in a simple JSON format.
     */
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, JdbcIdentity identity, List<SchemaTableName> tableNames)
    {
        if (tableNames.isEmpty()) {
            return ImmutableMap.of();
        }

        try (Connection connection = connectionFactory.openConnection(identity)) {
            Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();

            // Build the query to fetch view definitions from INFORMATION_SCHEMA.VIEWS
            StringBuilder sql = new StringBuilder(
                    "SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE (TABLE_SCHEMA, TABLE_NAME) IN (");

            List<String> placeholders = new ArrayList<>();
            for (int i = 0; i < tableNames.size(); i++) {
                placeholders.add("(?, ?)");
            }
            sql.append(String.join(", ", placeholders));
            sql.append(")");

            try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
                int paramIndex = 1;
                for (SchemaTableName tableName : tableNames) {
                    String remoteSchema = toRemoteSchemaName(session, identity, connection, tableName.getSchemaName());
                    String remoteTable = toRemoteTableName(session, identity, connection, remoteSchema, tableName.getTableName());
                    statement.setString(paramIndex++, remoteSchema);
                    statement.setString(paramIndex++, remoteTable);
                }

                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String viewSchemaName = resultSet.getString("TABLE_SCHEMA");
                        String viewTableName = resultSet.getString("TABLE_NAME");
                        String sqlServerViewSql = resultSet.getString("VIEW_DEFINITION");

                        // Fetch column metadata for the view
                        List<String> columnJsonList = new ArrayList<>();
                        try (ResultSet columnsResultSet = connection.getMetaData().getColumns(
                                connection.getCatalog(),
                                viewSchemaName,
                                viewTableName,
                                null)) {
                            while (columnsResultSet.next()) {
                                String columnName = columnsResultSet.getString("COLUMN_NAME");
                                JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                                        columnsResultSet.getInt("DATA_TYPE"),
                                        columnsResultSet.getString("TYPE_NAME"),
                                        columnsResultSet.getInt("COLUMN_SIZE"),
                                        columnsResultSet.getInt("DECIMAL_DIGITS"));

                                Optional<ReadMapping> readMapping = toPrestoType(session, typeHandle);
                                if (readMapping.isPresent()) {
                                    Type prestoType = readMapping.get().getType();
                                    // Normalize column name
                                    String normalizedColumnName = normalizeIdentifier(session, columnName);
                                    // Escape for JSON
                                    String escapedColumnName = normalizedColumnName
                                            .replace("\\", "\\\\")
                                            .replace("\"", "\\\"");
                                    String escapedTypeName = prestoType.getDisplayName()
                                            .replace("\\", "\\\\")
                                            .replace("\"", "\\\"");
                                    columnJsonList.add(String.format(
                                            "{\"name\":\"%s\",\"type\":\"%s\"}",
                                            escapedColumnName,
                                            escapedTypeName));
                                }
                            }
                        }

                        // Normalize identifiers according to SQL Server rules
                        viewSchemaName = normalizeIdentifier(session, viewSchemaName);
                        viewTableName = normalizeIdentifier(session, viewTableName);

                        SchemaTableName viewName = new SchemaTableName(viewSchemaName, viewTableName);

                        // Use session user as the view owner
                        String owner = session.getUser();

                        // Create a proper ViewDefinition JSON with all required fields including columns
                        String escapedSql = sqlServerViewSql
                                .replace("\\", "\\\\")
                                .replace("\"", "\\\"")
                                .replace("\n", "\\n")
                                .replace("\r", "\\r");

                        String columnsJson = String.join(",", columnJsonList);

                        String prestoViewData = String.format(
                                "{\"originalSql\":\"%s\",\"catalog\":\"%s\",\"schema\":\"%s\",\"columns\":[%s],\"owner\":\"%s\",\"runAsInvoker\":false}",
                                escapedSql,
                                connectorId,  // Use connector ID as catalog
                                viewSchemaName,
                                columnsJson,
                                owner);

                        views.put(viewName, new ConnectorViewDefinition(
                                viewName,
                                Optional.of(owner),
                                prestoViewData));
                    }
                }
            }

            return ImmutableMap.copyOf(views);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * List all views in the specified schema using JDBC metadata.
     */
    public List<SchemaTableName> listViews(ConnectorSession session, JdbcIdentity identity, Optional<String> schema)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            Optional<String> remoteSchema = schema.map(s -> toRemoteSchemaName(session, identity, connection, s));

            try (ResultSet resultSet = getTables(connection, remoteSchema, Optional.empty())) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    String tableType = resultSet.getString("TABLE_TYPE");
                    if ("VIEW".equals(tableType)) {
                        String schemaName = getTableSchemaName(resultSet);
                        String tableName = resultSet.getString("TABLE_NAME");

                        // Normalize identifiers according to SQL Server rules
                        schemaName = normalizeIdentifier(session, schemaName);
                        tableName = normalizeIdentifier(session, tableName);

                        list.add(new SchemaTableName(schemaName, tableName));
                    }
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * List all schemas that contain views.
     */
    public List<SchemaTableName> listSchemasForViews(ConnectorSession session, JdbcIdentity identity)
    {
        ImmutableList.Builder<SchemaTableName> allViews = ImmutableList.builder();
        for (String schema : getSchemaNames(session, identity)) {
            allViews.addAll(listViews(session, identity, Optional.of(schema)));
        }
        return allViews.build();
    }

    /**
     * Create a view in SQL Server.
     * Note: This method only creates the view in SQL Server database.
     * Presto will retrieve the actual view definition (with SQL Server's expanded SQL and column types)
     * when the view is first accessed, ensuring consistency.
     */
    public void createView(ConnectorSession session, JdbcIdentity identity, SchemaTableName viewName, String viewData, boolean replace)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            // Extract the original SQL from ViewDefinition JSON
            String viewSql = extractOriginalSql(viewData);

            // Remove catalog prefix from table references (sqlserver.schema.table -> schema.table)
            viewSql = removeCatalogPrefix(viewSql);

            String remoteSchema = toRemoteSchemaName(session, identity, connection, viewName.getSchemaName());
            String remoteTable = toRemoteTableName(session, identity, connection, remoteSchema, viewName.getTableName());

            String sql;
            if (replace) {
                // SQL Server doesn't have CREATE OR REPLACE VIEW, so we need to drop first if it exists
                String dropSql = format("IF OBJECT_ID('%s.%s', 'V') IS NOT NULL DROP VIEW %s.%s",
                        quoted(remoteSchema),
                        quoted(remoteTable),
                        quoted(remoteSchema),
                        quoted(remoteTable));
                execute(connection, dropSql);
            }

            sql = format("CREATE VIEW %s.%s AS %s",
                    quoted(remoteSchema),
                    quoted(remoteTable),
                    viewSql);

            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * Remove catalog prefix from table references in SQL.
     */
    private String removeCatalogPrefix(String sql)
    {
        // Pattern to match: word.word.word (catalog.schema.table)
        // Replace with: word.word (schema.table)
        return sql.replaceAll("\\b\\w+\\.(\\w+\\.\\w+)\\b", "$1");
    }

    /**
     * Extract the originalSql from ViewDefinition JSON string.
     */
    private String extractOriginalSql(String viewData)
    {
        int startIndex = viewData.indexOf("\"originalSql\":\"");
        if (startIndex == -1) {
            throw new PrestoException(JDBC_ERROR, "Invalid view data: missing originalSql");
        }
        startIndex += "\"originalSql\":\"".length();

        StringBuilder sql = new StringBuilder();
        boolean escaped = false;
        for (int i = startIndex; i < viewData.length(); i++) {
            char c = viewData.charAt(i);
            if (escaped) {
                if (c == 'n') {
                    sql.append('\n');
                }
                else if (c == 't') {
                    sql.append('\t');
                }
                else if (c == 'r') {
                    sql.append('\r');
                }
                else if (c == '"' || c == '\\') {
                    sql.append(c);
                }
                else {
                    sql.append('\\').append(c);
                }
                escaped = false;
            }
            else if (c == '\\') {
                escaped = true;
            }
            else if (c == '"') {
                break;
            }
            else {
                sql.append(c);
            }
        }

        return sql.toString();
    }

    /**
     * Drop a view in SQL Server.
     */
    public void dropView(ConnectorSession session, JdbcIdentity identity, SchemaTableName viewName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String remoteSchema = toRemoteSchemaName(session, identity, connection, viewName.getSchemaName());
            String remoteTable = toRemoteTableName(session, identity, connection, remoteSchema, viewName.getTableName());

            String sql = format("DROP VIEW %s.%s",
                    quoted(remoteSchema),
                    quoted(remoteTable));

            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private static String singleQuote(String... objects)
    {
        return singleQuote(DOT_JOINER.join(objects));
    }

    private static String singleQuote(String literal)
    {
        return "\'" + literal + "\'";
    }

    @Override
    public String normalizeIdentifier(ConnectorSession session, String identifier)
    {
        return caseSensitiveNameMatchingEnabled ? identifier : identifier.toLowerCase(ENGLISH);
    }
}
