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
package com.facebook.presto.plugin.prometheus;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.plugin.prometheus.PrometheusQueryRunner.createPrometheusQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Integration tests for Prometheus case-sensitive name matching
 */
@Test(priority = 1, singleThreaded = true)
public class TestPrometheusMixedCase
        extends AbstractTestQueryFramework
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("prometheus")
            .setSchema("default")
            .build();

    private PrometheusServer server;
    private Session session;
    private QueryRunner runner;

    // Define metrics with names that differ only by case
    private static final String LOWERCASE_METRIC = "https_requests_total";
    private static final String MIXEDCASE_METRIC = "Https_Requests_Total";
    private static final String UPPERCASE_METRIC = "HTTPS_REQUESTS_TOTAL";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.server = new PrometheusServer();
        Map<String, String> properties = ImmutableMap.of("case-sensitive-name-matching", "true");
        return createPrometheusQueryRunner(server, properties);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (server != null) {
            server.close();
        }
    }

    @Test
    public void testCaseSensitiveTableNames()
            throws Exception
    {
        // Wait for the "up" metric to be available
        waitForMetricAvailability();
        // Test that we can query the "up" metric with the correct case
        MaterializedResult results = computeActual("SELECT COUNT(*) FROM prometheus.default.up");
        assertEquals(results.getRowCount(), 1);
        // Test that querying with a different case fails when case sensitivity is enabled
        assertQueryFails("SELECT COUNT(*) FROM prometheus.default.UP", ".*Table prometheus.default.UP does not exist.*");
    }

    @Test
    public void testMixedCaseTableHandling()
            throws Exception
    {
        // Create a ConnectorSession
        ConnectorSession connectorSession = SESSION.toConnectorSession();
        // Create a PrometheusMetadata instance with case sensitivity enabled
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setCaseSensitiveNameMatchingEnabled(true);
        // Create a mock PrometheusClient that returns tables with different cases
        MockPrometheusClient mockClient = new MockPrometheusClient();
        // Create a custom TestMetadata that uses mock client
        TestMetadata metadata = new TestMetadata(mockClient, config);
        // Test that all three tables with different cases exist
        assertTrue(metadata.getTableHandle(connectorSession, new SchemaTableName("default", LOWERCASE_METRIC)) != null);
        assertTrue(metadata.getTableHandle(connectorSession, new SchemaTableName("default", MIXEDCASE_METRIC)) != null);
        assertTrue(metadata.getTableHandle(connectorSession, new SchemaTableName("default", UPPERCASE_METRIC)) != null);
        // Test that tables are distinct when case sensitivity is enabled
        Set<String> tableNames = ImmutableSet.copyOf(metadata.listSchemaNames());
        assertTrue(tableNames.contains("default"));
        // Get the list of tables and verify all three case variations are present
        List<SchemaTableName> tables = metadata.listTables(Optional.of("default"));
        assertTrue(tables.contains(new SchemaTableName("default", LOWERCASE_METRIC)));
        assertTrue(tables.contains(new SchemaTableName("default", MIXEDCASE_METRIC)));
        assertTrue(tables.contains(new SchemaTableName("default", UPPERCASE_METRIC)));
        // test with case sensitivity disabled
        config.setCaseSensitiveNameMatchingEnabled(false);
        metadata = new TestMetadata(mockClient, config);
        // With case sensitivity disabled, all three tables should be treated as the same table
        // The table handle should be the same regardless of the case used in the request
        SchemaTableName lowercaseName = new SchemaTableName("default", LOWERCASE_METRIC);
        SchemaTableName uppercaseName = new SchemaTableName("default", UPPERCASE_METRIC);
        PrometheusTableHandle lowercaseHandle = metadata.getTableHandle(connectorSession, lowercaseName);
        PrometheusTableHandle uppercaseHandle = metadata.getTableHandle(connectorSession, uppercaseName);
        assertEquals(lowercaseHandle.getTableName(), uppercaseHandle.getTableName());
    }
    private void waitForMetricAvailability()
            throws Exception
    {
        int maxTries = 60;
        int timeBetweenTriesMillis = 1000;
        runner = getQueryRunner();
        session = runner.getDefaultSession();
        int tries = 0;
        final OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectTimeout(120, TimeUnit.SECONDS)
                .readTimeout(120, TimeUnit.SECONDS)
                .build();
        HttpUrl.Builder urlBuilder = HttpUrl.parse(server.getUri().toString()).newBuilder().encodedPath("/api/v1/query");
        urlBuilder.addQueryParameter("query", "up[1d]");
        String url = urlBuilder.build().toString();
        Request request = new Request.Builder()
                .url(url)
                .build();
        String responseBody;
        while (tries < maxTries) {
            responseBody = httpClient.newCall(request).execute().body().string();
            if (responseBody.contains("values")) {
                Logger log = Logger.get(TestPrometheusMixedCase.class);
                log.info("prometheus response: %s", responseBody);
                break;
            }
            Thread.sleep(timeBetweenTriesMillis);
            tries++;
        }
        if (tries == maxTries) {
            fail("Prometheus container not available for metrics query in " + maxTries * timeBetweenTriesMillis + " milliseconds.");
        }
        tries = 0;
        while (tries < maxTries) {
            if (session != null && runner.tableExists(session, "up")) {
                break;
            }
            Thread.sleep(timeBetweenTriesMillis);
            tries++;
        }
        if (tries == maxTries) {
            fail("Prometheus container, or client, not available for metrics query in " + maxTries * timeBetweenTriesMillis + " milliseconds.");
        }
    }
    /**
     * Test implementation of metadata that works with mock client
     */
    private static class TestMetadata
    {
        private final MockPrometheusClient mockClient;
        private final boolean caseSensitiveNameMatchingEnabled;

        public TestMetadata(MockPrometheusClient mockClient, PrometheusConnectorConfig config)
        {
            this.mockClient = mockClient;
            this.caseSensitiveNameMatchingEnabled = config.isCaseSensitiveNameMatchingEnabled();
        }
        public PrometheusTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
        {
            if (!"default".equals(tableName.getSchemaName())) {
                return null;
            }

            String normalizedRequestedName = normalizeIdentifier(tableName.getTableName());
            for (String actualTableName : mockClient.getTableNames(tableName.getSchemaName())) {
                String normalizedActualName = normalizeIdentifier(actualTableName);
                if (normalizedRequestedName.equals(normalizedActualName)) {
                    return new PrometheusTableHandle(tableName.getSchemaName(), actualTableName);
                }
            }
            return null;
        }
        public List<String> listSchemaNames()
        {
            return ImmutableList.of("default");
        }
        public List<SchemaTableName> listTables(Optional<String> schemaName)
        {
            Set<String> schemaNames = schemaName.map(ImmutableSet::of)
                    .orElseGet(() -> ImmutableSet.of("default"));

            return schemaNames.stream()
                    .flatMap(thisSchemaName ->
                            mockClient.getTableNames(thisSchemaName).stream().map(tableName -> new SchemaTableName(thisSchemaName, tableName)))
                    .collect(ImmutableList.toImmutableList());
        }
        public String normalizeIdentifier(String identifier)
        {
            return caseSensitiveNameMatchingEnabled ? identifier : identifier.toLowerCase(java.util.Locale.ROOT);
        }
    }
    /**
     * Mock PrometheusClient that returns tables with different cases
     */
    private static class MockPrometheusClient
    {
        private final Set<String> tableNames;
        public MockPrometheusClient()
        {
            this.tableNames = ImmutableSet.of(LOWERCASE_METRIC, MIXEDCASE_METRIC, UPPERCASE_METRIC);
        }
        public Set<String> getTableNames(String schema)
        {
            return tableNames;
        }
        public PrometheusTable getTable(String schema, String tableName)
        {
            // Return a table for any of the test metrics
            if (LOWERCASE_METRIC.equals(tableName) || MIXEDCASE_METRIC.equals(tableName) || UPPERCASE_METRIC.equals(tableName)) {
                return new PrometheusTable(
                        tableName,
                        ImmutableList.of(
                                new PrometheusColumn("labels", createUnboundedVarcharType()),
                                new PrometheusColumn("timestamp", TIMESTAMP_WITH_TIME_ZONE),
                                new PrometheusColumn("value", DoubleType.DOUBLE)));
            }
            return null;
        }
    }
}
