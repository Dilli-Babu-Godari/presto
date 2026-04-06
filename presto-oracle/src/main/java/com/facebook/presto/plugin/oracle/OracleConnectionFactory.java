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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import oracle.jdbc.OracleConnection;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

/**
 * Oracle-specific ConnectionFactory that sets default row prefetch on connections.
 * This wrapper applies Oracle's fetch size optimization at the connection level,
 * ensuring all statements and result sets inherit the configured fetch size.
 */
public class OracleConnectionFactory
        implements ConnectionFactory
{
    private final ConnectionFactory delegate;
    private final int fetchSize;

    public OracleConnectionFactory(ConnectionFactory delegate, int fetchSize)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.fetchSize = fetchSize;
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        Connection connection = delegate.openConnection(identity);

        // Set Oracle-specific default row prefetch for optimal performance
        if (connection.isWrapperFor(OracleConnection.class)) {
            OracleConnection oracleConnection = connection.unwrap(OracleConnection.class);
            oracleConnection.setDefaultRowPrefetch(fetchSize);
        }

        return connection;
    }

    @Override
    public void close()
            throws SQLException
    {
        delegate.close();
    }
}
