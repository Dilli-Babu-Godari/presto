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

import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataCache;
import com.facebook.presto.plugin.jdbc.TableLocationProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SqlServerMetadata
        extends JdbcMetadata
{
    private final SqlServerClient sqlServerClient;

    public SqlServerMetadata(JdbcMetadataCache jdbcMetadataCache, SqlServerClient client, boolean allowDropTable, TableLocationProvider tableLocationProvider)
    {
        super(jdbcMetadataCache, client, allowDropTable, tableLocationProvider);
        this.sqlServerClient = requireNonNull(client, "client is null");
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tableNames;
        boolean isInformationSchemaQuery = false;

        if (prefix.getTableName() != null) {
            tableNames = ImmutableList.of(
                    new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            isInformationSchemaQuery = true;
            if (prefix.getSchemaName() != null) {
                tableNames = listViews(session, Optional.of(prefix.getSchemaName()));
            }
            else {
                tableNames = sqlServerClient.listSchemasForViews(session);
            }
        }

        if (session.getQueryType().isPresent() || isInformationSchemaQuery) {
            return sqlServerClient.getViews(session, tableNames);
        }
        return super.getViews(session, prefix);
    }
}
