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

import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SetTimeZone;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.TimeZone;

import static com.facebook.presto.SystemSessionProperties.TIME_ZONE_ID;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class SetTimeZoneTask
        implements SessionTransactionControlTask<SetTimeZone>
{
    @Override
    public String getName()
    {
        return "SET TIME ZONE";
    }

    @Override
    public ListenableFuture<?> execute(
            SetTimeZone statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            String query)
    {
        requireNonNull(statement, "statement is null");
        
        String timeZoneId;
        
        if (statement.isLocal()) {
            // For LOCAL, use the JVM/system default time zone
            timeZoneId = TimeZone.getDefault().getID();
        }
        else {
            // Extract time zone from the provided expression
            Expression value = statement.getValue()
                    .orElseThrow(() -> new IllegalStateException("Time zone value must be present when not LOCAL"));
            
            if (!(value instanceof StringLiteral)) {
                throw new SemanticException(TYPE_MISMATCH, statement, "Time zone value must be a string literal");
            }
            
            timeZoneId = ((StringLiteral) value).getValue();
        }
        
        // Validate the time zone using existing validation logic
        // This will throw TimeZoneNotSupportedException if invalid
        TimeZoneKey.getTimeZoneKey(timeZoneId);
        
        // Directly set the session property
        stateMachine.addSetSessionProperties(ImmutableMap.of(TIME_ZONE_ID, timeZoneId));
        
        return immediateFuture(null);
    }
}
