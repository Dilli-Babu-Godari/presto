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
package com.facebook.presto.iceberg.procedure;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.HiveTransactionHandle;
import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.iceberg.IcebergTransactionManager;
import com.facebook.presto.iceberg.vectors.IcebergVectorIndexBuilder;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * Procedure for building vector indexes from Iceberg table data.
 */
public class BuildVectorIndexProcedure
        implements Provider<Procedure>
{
    private static final Logger log = Logger.get(BuildVectorIndexProcedure.class);

    private final IcebergTransactionManager transactionManager;
    private final IcebergMetadataFactory metadataFactory;
    private final ConnectorPageSourceProvider pageSourceProvider;

    @Inject
    public BuildVectorIndexProcedure(
            IcebergTransactionManager transactionManager,
            IcebergMetadataFactory metadataFactory,
            ConnectorPageSourceProvider pageSourceProvider)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
    }

    @Override
    public Procedure get()
    {
        // Create the MethodHandle at runtime and bind to this instance so the arity matches
        MethodHandle handle = methodHandle(
                BuildVectorIndexProcedure.class,
                "buildVectorIndex",
                ConnectorSession.class,
                String.class,    // schemaName
                String.class,    // tableName
                String.class,    // columnName
                String.class,    // similarityFunction
                Integer.class,   // m
                Integer.class    // efConstruction
        ).bindTo(this);

        return new Procedure(
                "system",
                "build_vector_index",
                ImmutableList.of(
                        new Procedure.Argument("schema_name", VARCHAR),
                        new Procedure.Argument("table_name", VARCHAR),
                        new Procedure.Argument("column_name", VARCHAR),
                        new Procedure.Argument("similarity_function", VARCHAR),
                        new Procedure.Argument("m", INTEGER),
                        new Procedure.Argument("ef_construction", INTEGER)),
                handle);
    }

    public void buildVectorIndex(
            ConnectorSession session,
            String schemaName,
            String tableName,
            String columnName,
            String similarityFunction,
            Integer m,
            Integer efConstruction)
    {
        log.info("Building vector index for %s.%s.%s", schemaName, tableName, columnName);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        // Default values if null
        int mValue = (m != null) ? m : 16;
        int efValue = (efConstruction != null) ? efConstruction : 100;
        String simFunction = (similarityFunction != null) ? similarityFunction : "COSINE";

        // Create a new transaction handle for this procedure
        ConnectorTransactionHandle transactionHandle = new HiveTransactionHandle();
        boolean transactionRegistered = false;

        try {
            // Create a new metadata instance and register it with the transaction manager
            ConnectorMetadata metadata = metadataFactory.create();
            transactionManager.put(transactionHandle, metadata);
            transactionRegistered = true;

            Path resultPath = IcebergVectorIndexBuilder.buildAndSaveVectorIndex(
                    metadata,
                    pageSourceProvider,
                    transactionHandle,
                    session,
                    schemaTableName,
                    columnName,
                    simFunction,
                    mValue,
                    efValue);
            log.info("Vector index built and saved to %s", resultPath);
        }
        catch (Exception e) {
            log.error(e, "Error building vector index");
            throw new RuntimeException("Error building vector index: " + e.getMessage(), e);
        }
        finally {
            // Only clean up the transaction if it was successfully registered
            if (transactionRegistered) {
                try {
                    transactionManager.remove(transactionHandle);
                }
                catch (Exception e) {
                    log.warn(e, "Failed to remove transaction handle");
                }
            }
        }
    }
}
