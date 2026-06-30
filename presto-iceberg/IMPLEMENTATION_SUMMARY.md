# Temporary Credentials Cache - Implementation Summary

## Overview

This implementation provides a coordinator-level cache for managing temporary storage credentials obtained from an Iceberg REST Catalog. The cache handles credential lifecycle, proactive refresh, and distribution to workers.

## What is Guava Cache?

**Guava** is Google's core libraries for Java, providing utilities for collections, caching, primitives support, concurrency, common annotations, string processing, I/O, and more.

**Current Version in Presto**: `32.1.0-jre` (defined in root `pom.xml`)

**Guava Cache** (`com.google.common.cache`) is a high-performance, thread-safe, in-memory caching library that provides:
- Automatic loading of entries into the cache
- Size-based eviction (maximumSize, maximumWeight)
- Time-based expiration (expireAfterWrite, expireAfterAccess)
- Removal notifications
- Cache statistics
- Thread-safe operations

### Why Guava Cache?

1. **Already in Presto** - No new dependencies needed
2. **Proven Pattern** - Used extensively throughout Presto:
   - [`ManifestFileCache`](presto_oss/presto/presto-iceberg/src/main/java/com/facebook/presto/iceberg/ManifestFileCache.java) - Caches Iceberg manifest files
   - [`IcebergNativeCatalogFactory`](presto_oss/presto/presto-iceberg/src/main/java/com/facebook/presto/iceberg/IcebergNativeCatalogFactory.java) - Caches catalog instances
   - [`HiveTableOperations`](presto_oss/presto/presto-iceberg/src/main/java/com/facebook/presto/iceberg/HiveTableOperations.java) - Caches commit locks
   - [`StatisticsFileCache`](presto_oss/presto/presto-iceberg/src/main/java/com/facebook/presto/iceberg/statistics/StatisticsFileCache.java) - Caches statistics files
3. **Production-Ready** - Battle-tested in high-scale environments
4. **Feature-Rich** - Provides all needed features out of the box

## Implementation Components

### 1. Data Model

**[`TemporaryCredential.java`](presto_oss/presto/presto-iceberg/src/main/java/com/facebook/presto/iceberg/rest/TemporaryCredential.java)**
```java
public class TemporaryCredential {
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
    private final Instant expirationTime;
    private final String storageLocation;
    
    // Methods for expiration checking
    public boolean isExpired()
    public boolean needsRefresh(Duration bufferTime)
    public Duration getTimeUntilExpiration()
}
```

### 2. Cache Key

**[`CredentialCacheKey.java`](presto_oss/presto/presto-iceberg/src/main/java/com/facebook/presto/iceberg/rest/CredentialCacheKey.java)**
```java
public class CredentialCacheKey {
    private final String catalogName;
    private final Optional<String> userId;  // For USER session type
}
```

**Strategy**: Per-catalog caching with optional user-level isolation

### 3. Cache Implementation

**[`TemporaryCredentialsCache.java`](presto_oss/presto/presto-iceberg/src/main/java/com/facebook/presto/iceberg/rest/TemporaryCredentialsCache.java)**

Key features:
```java
// Guava LoadingCache with automatic loading
private final LoadingCache<CredentialCacheKey, TemporaryCredential> cache;

// Background refresh executor
private final ScheduledExecutorService refreshExecutor;

// Striped locks for concurrent refresh protection
private final Striped<Lock> refreshLocks = Striped.lock(16);
```

**Cache Configuration**:
```java
this.cache = CacheBuilder.newBuilder()
    .maximumSize(config.getCredentialCacheSize())  // Size-based eviction
    .expireAfterWrite(Duration.ofHours(1))         // Time-based safety net
    .removalListener(new CredentialRemovalListener())  // Removal notifications
    .recordStats()                                  // Enable statistics
    .build(new CredentialCacheLoader());           // Automatic loading
```

### 4. Configuration Properties

**Added to [`IcebergRestConfig.java`](presto_oss/presto/presto-iceberg/src/main/java/com/facebook/presto/iceberg/rest/IcebergRestConfig.java)**:

| Property | Default | Description |
|----------|---------|-------------|
| `iceberg.rest.credential.caching-enabled` | `true` | Enable/disable credential caching |
| `iceberg.rest.credential.cache-size` | `10` | Maximum number of cached credentials |
| `iceberg.rest.credential.refresh-buffer` | `5 minutes` | Time before expiration to trigger refresh |

## How It Works

### 1. Credential Fetch Flow

```
User Query
    ↓
Coordinator needs credentials
    ↓
Check cache (Guava LoadingCache)
    ↓
    ├─ Cache Hit → Return cached credential
    │              Check if needs refresh
    │              Schedule background refresh if needed
    │
    └─ Cache Miss → Fetch from REST Catalog
                    Validate credential
                    Store in cache
                    Return credential
```

### 2. Proactive Refresh

```
Background Thread (runs every 1 minute)
    ↓
Scan all cached credentials
    ↓
For each credential nearing expiration:
    ↓
Acquire striped lock for key
    ↓
Check if still needs refresh (double-check)
    ↓
Fetch new credentials from REST Catalog
    ↓
Update cache atomically
    ↓
Release lock
```

### 3. Concurrent Access Protection

Uses **Guava Striped Locks**:
```java
private final Striped<Lock> refreshLocks = Striped.lock(16);

private void refreshCredential(CredentialCacheKey key) {
    Lock lock = refreshLocks.get(key);
    lock.lock();
    try {
        // Double-check if refresh still needed
        // Fetch and update credentials
    }
    finally {
        lock.unlock();
    }
}
```

**Benefits**:
- Prevents duplicate refreshes for the same key
- Allows concurrent refreshes for different keys
- Low contention with 16 lock stripes

### 4. Error Handling

**Refresh Failure Strategy**:
```java
try {
    // Fetch new credentials
    cache.put(key, newCredential);
    refreshSuccessCount.incrementAndGet();
}
catch (Exception e) {
    refreshFailureCount.incrementAndGet();
    
    TemporaryCredential existing = cache.getIfPresent(key);
    if (existing != null && existing.isValid()) {
        // Continue with existing credential
        log.warn("Refresh failed, using existing credential");
    }
    else {
        // Critical failure - no valid credential
        cache.invalidate(key);
        throw new PrestoException(ICEBERG_CREDENTIAL_ERROR, ...);
    }
}
```

## Monitoring & Observability

### JMX Metrics

The cache exposes the following JMX metrics:

```java
@Managed
public CacheStatsMBean getCacheStats()  // Guava cache statistics

@Managed
public long getActiveCredentials()  // Current cache size

@Managed
public long getCredentialsNearingExpiration()  // Count needing refresh

@Managed
public long getRefreshSuccessCount()  // Total successful refreshes

@Managed
public long getRefreshFailureCount()  // Total failed refreshes
```

**Guava Cache Statistics** (via `CacheStatsMBean`):
- Hit rate / Miss rate
- Load success / Load failure count
- Average load time
- Eviction count
- Total load time

## Comparison with Other Presto Caches

| Cache | Type | Eviction | Expiration | Refresh | Use Case |
|-------|------|----------|------------|---------|----------|
| **TemporaryCredentialsCache** | LoadingCache | Size | Time + Safety | Proactive | Short-lived credentials |
| ManifestFileCache | Cache | Weight | Time | None | Manifest file content |
| Catalog Cache | Cache | Size | None | None | Catalog instances |
| Commit Lock Cache | LoadingCache | Access-based | None | None | Table-level locks |
| Statistics Cache | Cache | Weight | None | None | Column statistics |

## Integration Points

### Current Status

✅ **Implemented**:
- Core cache with Guava LoadingCache
- Credential data model
- Cache key strategy
- Proactive refresh mechanism
- Concurrent access protection
- Error handling with fallback
- Configuration properties
- JMX monitoring

⏳ **Remaining Work**:
1. **REST Catalog Integration** - Implement actual credential fetch from Iceberg REST Catalog API
2. **Module Binding** - Wire cache in `IcebergRestCatalogModule`
3. **Factory Integration** - Update `IcebergRestCatalogFactory` to use cache
4. **Worker Distribution** - Implement credential distribution to workers
5. **Testing** - Unit and integration tests

### Next Steps

1. **Understand REST Catalog API**:
   - What endpoint returns credentials?
   - What is the response format?
   - Are credentials per-warehouse or per-table?

2. **Implement Credential Fetcher**:
   ```java
   Function<CredentialCacheKey, TemporaryCredential> fetcher = key -> {
       // Call REST Catalog API
       // Parse response
       // Return TemporaryCredential
   };
   ```

3. **Wire in Module**:
   ```java
   @Singleton
   @Provides
   public TemporaryCredentialsCache createCache(
           IcebergRestConfig config,
           RESTCatalog restCatalog,
           MBeanExporter exporter) {
       Function<CredentialCacheKey, TemporaryCredential> fetcher = 
           key -> fetchFromRestCatalog(restCatalog, key);
       
       TemporaryCredentialsCache cache = 
           new TemporaryCredentialsCache(config, fetcher);
       
       exporter.export("presto.iceberg:name=TemporaryCredentialsCache", cache);
       return cache;
   }
   ```

4. **Update Factory**:
   ```java
   public class IcebergRestCatalogFactory {
       private final TemporaryCredentialsCache credentialsCache;
       
       public TemporaryCredential getCredentials(ConnectorSession session) {
           CredentialCacheKey key = new CredentialCacheKey(
               catalogName,
               getUserId(session)
           );
           return credentialsCache.getCredentials(key);
       }
   }
   ```

## Design Decisions

### Why Per-Catalog Caching?

**Chosen**: Per-catalog with optional user isolation

**Rationale**:
- REST Catalog typically returns warehouse-scoped credentials
- Simpler implementation with fewer cache entries
- Aligns with existing catalog cache pattern
- Can be extended to per-table if needed

**Alternative Considered**: Per-table caching
- More granular but higher cache overhead
- Requires table-level credential support from REST Catalog
- Can be added later if REST Catalog supports it

### Why 5-Minute Refresh Buffer?

**Chosen**: 5 minutes before expiration

**Rationale**:
- Provides safety margin for refresh failures
- Allows time for retry attempts
- Balances between freshness and refresh frequency
- Configurable via `iceberg.rest.credential.refresh-buffer`

### Why Striped Locks?

**Chosen**: Guava Striped with 16 locks

**Rationale**:
- Prevents duplicate refreshes for same key
- Allows concurrent refreshes for different keys
- Low memory overhead (16 locks vs. lock-per-key)
- Proven pattern in Presto (used in commit lock cache)

## References

- [Guava Cache Documentation](https://github.com/google/guava/wiki/CachesExplained)
- [Iceberg REST Catalog Spec](https://iceberg.apache.org/docs/latest/rest-catalog/)
- [Presto Iceberg Connector](https://prestodb.io/docs/current/connector/iceberg.html)
- [Design Document](TEMPORARY_CREDENTIALS_CACHE_DESIGN.md)