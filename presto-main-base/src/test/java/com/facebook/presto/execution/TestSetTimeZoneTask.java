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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SetTimeZone;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestSetTimeZoneTask
{
    private final Metadata metadata = createTestMetadataManager();
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-set-timezone-task-executor-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testSetTimeZoneWithString()
    {
        testSetTimeZone(new StringLiteral("America/Los_Angeles"), "America/Los_Angeles");
        testSetTimeZone(new StringLiteral("+05:30"), "+05:30");
        testSetTimeZone(new StringLiteral("UTC"), "UTC");
    }

    @Test
    public void testSetTimeZoneLocal()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE LOCAL", TEST_SESSION, false, transactionManager, executor, metadata);
        
        SetTimeZoneTask task = new SetTimeZoneTask();
        SetTimeZone statement = new SetTimeZone(Optional.empty(), true);
        
        getFutureValue(task.execute(statement, transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList(), "SET TIME ZONE LOCAL"));
        
        Map<String, String> sessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(sessionProperties.size(), 1);
        // LOCAL should resolve to JVM/system default time zone
        assertEquals(sessionProperties.get("time_zone_id"), java.util.TimeZone.getDefault().getID());
    }

    private void testSetTimeZone(Expression expression, String expectedValue)
    {
        TransactionManager transactionManager = createTestTransactionManager();
        String sql = "SET TIME ZONE '" + expectedValue + "'";
        QueryStateMachine stateMachine = createQueryStateMachine(sql, TEST_SESSION, false, transactionManager, executor, metadata);
        
        SetTimeZoneTask task = new SetTimeZoneTask();
        SetTimeZone statement = new SetTimeZone(Optional.of(expression), false);
        
        getFutureValue(task.execute(statement, transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList(), sql));
        
        Map<String, String> sessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(sessionProperties.size(), 1);
        assertEquals(sessionProperties.get("time_zone_id"), expectedValue);
    }

    private static QueryStateMachine createQueryStateMachine(String query, Session session, boolean transactionControl, TransactionManager transactionManager, ExecutorService executor, Metadata metadata)
    {
        return QueryStateMachine.begin(
                query,
                Optional.empty(),
                session,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                transactionControl,
                transactionManager,
                new AllowAllAccessControl(),
                executor,
                metadata,
                WarningCollector.NOOP,
                Optional.empty());
    }
}
