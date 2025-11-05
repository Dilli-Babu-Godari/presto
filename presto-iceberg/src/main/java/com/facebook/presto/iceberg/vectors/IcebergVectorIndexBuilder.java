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
package com.facebook.presto.iceberg.vectors;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergSplit;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.PartitionData;
import com.facebook.presto.iceberg.RuntimeStatsMetricsReporter;
import com.facebook.presto.iceberg.delete.DeleteFile;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
/**
 * Utility class for building vector indexes from Iceberg table data.
 */
public class IcebergVectorIndexBuilder
{
    private static final Logger log = Logger.get(IcebergVectorIndexBuilder.class);
    private static final String VECTOR_INDEX_BASE_DIR = "/tmp/vector_indexes";

    private IcebergVectorIndexBuilder() {}

    /**
     * Builds a vector index from an Iceberg table column and saves it to a file.
     * The index is saved to a path in the local filesystem:
     * /tmp/vector_indexes/[index_name]-[snapshot_id]-[timestamp].idx
     *
     * @param metadata The connector metadata
     * @param pageSourceProvider The page source provider
     * @param transactionHandle The transaction handle
     * @param session The connector session
     * @param schemaTableName The schema and table name
     * @param columnName The name of the column containing vector data
     * @param indexName The name of the index
     * @param catalogName The catalog name
     * @param similarityFunction The similarity function to use for the index
     * @param m The maximum number of connections per node in the graph
     * @param efConstruction The size of the dynamic candidate list during construction
     * @return The path to the saved index file
     */
    public static Path buildAndSaveVectorIndex(
            ConnectorMetadata metadata,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String columnName,
            String indexName,
            String catalogName,
            String similarityFunction,
            int m,
            int efConstruction) throws Exception
    {
        // 1. Get the Iceberg table
        Table icebergTable = IcebergUtil.getIcebergTable(metadata, session, schemaTableName);

        // Compute the local filesystem path for the index
        // Simple path format: /tmp/vector_indexes/[index_name]-[snapshot_id]-[timestamp].hnsw
        Path indexDirPath = Paths.get(VECTOR_INDEX_BASE_DIR);
        String indexFileName = indexName + ".hnsw";
        Path indexPath = indexDirPath.resolve(indexFileName);
        // Create the directory if it doesn't exist
        Files.createDirectories(indexDirPath);
        log.info("Vector index will be saved to local path: %s", indexPath);
        // 2. Read vectors from the specified column
        List<float[]> vectors = readVectorsFromTable(
                metadata,
                pageSourceProvider,
                transactionHandle,
                session,
                schemaTableName,
                columnName);
        if (vectors.isEmpty()) {
            throw new IllegalStateException("No vectors found in column: " + columnName);
        }
        // Normalize all vectors using L2 normalization
        log.info("Normalizing vectors using L2 normalization");
        for (float[] vector : vectors) {
            CustomVectorFloat customVector = new CustomVectorFloat(vector);
            VectorUtil.l2normalize(customVector.toArrayVectorFloat());
        }
        // 3. Create vector values wrapper
        int dimension = vectors.get(0).length;
        RandomAccessVectorValues ravv = new ListRandomAccessVectorValues(vectors, dimension);
        // 4. Create similarity function
        VectorSimilarityFunction simFunction = getVectorSimilarityFunction(similarityFunction);
        // 5. Build the index
        log.info("Building vector index with %d vectors of dimension %d", vectors.size(), dimension);
        // Create a BuildScoreProvider from the RandomAccessVectorValues and similarity function
        BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(ravv, simFunction);
        GraphIndexBuilder builder = new GraphIndexBuilder(bsp, ravv.dimension(), m, efConstruction, 4 * m, 1.2f, false, true);
        ImmutableGraphIndex index = builder.build(ravv);
        log.info("Vector index built successfully with %d nodes", index.size());
        // 6. Save index directly to local filesystem
        log.info("Writing index to filesystem: %s", indexPath);
        OnDiskGraphIndex.write(index, ravv, indexPath);
        log.info("Vector index saved successfully to %s", indexPath);
        return indexPath;
    }
    private static VectorSimilarityFunction getVectorSimilarityFunction(String similarityFunction)
    {
        switch (similarityFunction.toUpperCase()) {
            case "COSINE":
                return VectorSimilarityFunction.COSINE;
            case "DOT_PRODUCT":
                return VectorSimilarityFunction.DOT_PRODUCT;
            case "EUCLIDEAN":
                return VectorSimilarityFunction.EUCLIDEAN;
            default:
                throw new IllegalArgumentException("Unsupported similarity function: " + similarityFunction);
        }
    }
    /**
     * Reads vector data from an Iceberg table column.
     */
    private static List<float[]> readVectorsFromTable(
            ConnectorMetadata metadata,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String columnName) throws IOException
    {
        // Get the table handle
        IcebergTableHandle tableHandle = (IcebergTableHandle) metadata.getTableHandle(session, schemaTableName);
        if (tableHandle == null) {
            throw new IllegalArgumentException("Table not found: " + schemaTableName);
        }
        // Find the column
        // TypeManager is not accessible from ConnectorSession
        // For now, we'll use the one from the metadata
        TypeManager typeManager = null;
        if (metadata instanceof IcebergAbstractMetadata) {
            typeManager = ((IcebergAbstractMetadata) metadata).getTypeManager();
        }
        if (typeManager == null) {
            throw new IllegalStateException("Could not get TypeManager from metadata");
        }
        // Get the Iceberg table
        Table icebergTable = IcebergUtil.getIcebergTable(metadata, session, schemaTableName);
        // Get all columns
        List<IcebergColumnHandle> columns = IcebergUtil.getColumns(
                icebergTable.schema(),
                icebergTable.spec(),
                typeManager);
        // Find the target column
        IcebergColumnHandle targetColumn = null;
        for (IcebergColumnHandle column : columns) {
            if (column.getName().equals(columnName)) {
                targetColumn = column;
                break;
            }
        }
        if (targetColumn == null) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }
        // Verify column type
        Type columnType = targetColumn.getType();
        if (!(columnType instanceof ArrayType)) {
            throw new IllegalArgumentException("Column must be an array type: " + columnName);
        }
        // Create a table layout handle
        IcebergTableLayoutHandle layoutHandle = createTableLayoutHandle(tableHandle, columns);
        // Use TableScan API to get actual data files
        List<float[]> vectors = new ArrayList<>();
        // Create a table scan to get the data files
        TableScan tableScan = icebergTable.newScan()
                .metricsReporter(new RuntimeStatsMetricsReporter(session.getRuntimeStats()));
        // If the table has a current snapshot, use it
        if (icebergTable.currentSnapshot() != null) {
            tableScan = tableScan.useSnapshot(icebergTable.currentSnapshot().snapshotId());
        }
        // Get the data files
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
                // Create a split for each data file
                IcebergSplit split = new IcebergSplit(
                        fileScanTask.file().path().toString(), // Use the actual file path
                        fileScanTask.start(),
                        fileScanTask.length(),
                        IcebergUtil.getFileFormat(icebergTable),
                        ImmutableList.of(),
                        IcebergUtil.getPartitionKeys(fileScanTask),
                        PartitionSpecParser.toJson(fileScanTask.spec()),
                        IcebergUtil.partitionDataFromStructLike(fileScanTask.spec(), fileScanTask.file().partition()).map(PartitionData::toJson),
                        NodeSelectionStrategy.NO_PREFERENCE,
                        SplitWeight.standard(),
                        fileScanTask.deletes().stream().map(DeleteFile::fromIceberg).collect(toImmutableList()),
                        Optional.empty(),
                        IcebergUtil.getDataSequenceNumber(fileScanTask.file()),
                        1); // Use 1 to avoid division by zero
                // Read the data from this split
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                        transactionHandle,
                        session,
                        split,
                        layoutHandle,
                        ImmutableList.of(targetColumn),
                        new SplitContext(false),
                        new RuntimeStats())) {
                    while (!pageSource.isFinished()) {
                        Page page = pageSource.getNextPage();
                        if (page == null) {
                            continue;
                        }
                        Block block = page.getBlock(0);
                        for (int position = 0; position < block.getPositionCount(); position++) {
                            if (block.isNull(position)) {
                                continue;
                            }
                            Block arrayBlock = block.getBlock(position);
                            if (arrayBlock.getPositionCount() == 0) {
                                continue;
                            }
                            float[] vector = new float[arrayBlock.getPositionCount()];
                            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                                if (arrayBlock.isNull(i)) {
                                    vector[i] = 0.0f;
                                }
                                else {
                                    vector[i] = ((Number) ((ArrayType) columnType).getElementType().getObjectValue(
                                            session.getSqlFunctionProperties(), arrayBlock, i)).floatValue();
                                }
                            }
                            vectors.add(vector);
                        }
                    }
                }
            }
        }
        return vectors;
    }
    /**
     * Creates a minimal IcebergTableLayoutHandle for reading data.
     */
    private static IcebergTableLayoutHandle createTableLayoutHandle(IcebergTableHandle tableHandle, List<IcebergColumnHandle> columns)
    {
        // Create a minimal table layout handle with required fields
        return new IcebergTableLayoutHandle.Builder()
                .setPartitionColumns(ImmutableList.of())
                .setDataColumns(ImmutableList.of())
                .setDomainPredicate(TupleDomain.all())
                .setRemainingPredicate(new ConstantExpression(true, BooleanType.BOOLEAN))
                .setPredicateColumns(ImmutableMap.of())
                .setRequestedColumns(Optional.empty())
                .setPushdownFilterEnabled(false)
                .setPartitionColumnPredicate(TupleDomain.all())
                .setPartitions(Optional.empty())
                .setTable(tableHandle)
                .build();
    }
}
