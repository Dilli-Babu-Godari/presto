# Temporary Credentials Cache Design for Iceberg REST Catalog

## Executive Summary

This document outlines the design and implementation strategy for a coordinator-level cache that manages temporary storage credentials obtained from an Iceberg REST Catalog. The cache will handle credential lifecycle, proactive refresh, and distribution to workers.

## Background

Currently, Presto uses static storage credentials configured in catalog properties. This implementation introduces dynamic temporary credentials with automatic refresh, improving security by:
- Eliminating long-lived credentials in configuration files
- Supporting short-lived, scoped credentials
- Enabling automatic credential rotation
- Abstracting cloud-provider-specific credential generation

## Architecture Overview

```
Coordinator
    │
    ├─> IcebergRestCatalogFactory
    │       │
    │       └─> TemporaryCredentialsCache (NEW)
    │               │
    │               ├─> Credential Storage
    │               ├─> Expiration Tracking
    │               └─> Proactive Refresh
    │
    └─> Workers (via TaskUpdateRequest)
            │
            └─> S3 Client (with temporary credentials)
```

## Existing Cache Patterns in Presto

### 1. ManifestFileCache Pattern
**Location**: `presto-iceberg/src/main/java/com/facebook/presto/iceberg/ManifestFileCache.java`

**Key Features**:
- Extends `ForwardingCache.SimpleForwardingCache`
- Uses Guava `CacheBuilder` with:
  - `maximumWeight()` for size-based eviction
  - `expireAfterWrite()` for time-based eviction
  - Custom weigher for accurate memory tracking
- Integrated with JMX via `CacheStatsMBean`
- Configured via `IcebergConfig` properties

**Relevant Code**:
```java
CacheBuilder<ManifestFileCacheKey, ManifestFileCachedContent> delegate = CacheBuilder.newBuilder()
    .maximumWeight(config.getMaxManifestCacheSize())
    .<ManifestFileCacheKey, ManifestFileCachedContent>weigher((key, entry) -> ...)
    .recordStats()
    .expireAfterWrite(Duration.ofMillis(config.getManifestCacheExpireDuration()))
    .build();
```

### 2. Catalog Cache Pattern
**Location**: `presto-iceberg/src/main/java/com/facebook/presto/iceberg/IcebergNativeCatalogFactory.java`

**Key Features**:
- Simple `Cache<String, Catalog>` with `maximumSize()`
- Cache key generation via `getCacheKey(session)`
- Lazy loading with `cache.get(key, callable)`
- Session-aware caching

**Relevant Code**:
```java
protected final Cache<String, Catalog> catalogCache;

catalogCache = CacheBuilder.newBuilder()
    .maximumSize(config.getCatalogCacheSize())
    .build();

public Catalog getCatalog(ConnectorSession session) {
    return catalogCache.get(getCacheKey(session), () -> loadCatalog(...));
}
```

### 3. Commit Lock Cache Pattern
**Location**: `presto-iceberg/src/main/java/com/facebook/presto/iceberg/HiveTableOperations.java`

**Key Features**:
- `LoadingCache` with automatic value creation
- `expireAfterAccess()` for idle eviction
- Thread-safe lock management
- Singleton pattern with synchronized initialization

**Relevant Code**:
```java
private static LoadingCache<String, ReentrantLock> commitLockCache;

commitLockCache = CacheBuilder.newBuilder()
    .expireAfterAccess(evictionTimeout, TimeUnit.MILLISECONDS)
    .build(new CacheLoader<String, ReentrantLock>() {
        @Override
        public ReentrantLock load(String fullName) {
            return new ReentrantLock();
        }
    });
```

## Proposed Implementation

### 1. Credential Data Model

```java
public class TemporaryCredential {
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
    private final Instant expirationTime;
    private final String storageLocation;  // S3 bucket/prefix
    
    public boolean isExpired() {
        return Instant.now().isAfter(expirationTime);
    }
    
    public boolean needsRefresh(Duration bufferTime) {
        return Instant.now().plus(bufferTime).isAfter(expirationTime);
    }
    
    public Duration getTimeUntilExpiration() {
        return Duration.between(Instant.now(), expirationTime);
    }
}
```

### 2. Cache Key Strategy

**Decision**: Use **per-catalog** caching strategy

**Rationale**:
- REST Catalog typically returns credentials scoped to the entire warehouse
- Simpler implementation with fewer cache entries
- Aligns with existing `IcebergRestCatalogFactory` architecture
- Can be extended to per-table if needed in future

**Cache Key Structure**:
```java
public class CredentialCacheKey {
    private final String catalogName;
    private final Optional<String> userId;  // For USER session type
    
    @Override
    public boolean equals(Object o) { ... }
    
    @Override
    public int hashCode() { ... }
}
```

### 3. Cache Implementation

```java
public class TemporaryCredentialsCache {
    private final LoadingCache<CredentialCacheKey, TemporaryCredential> cache;
    private final ScheduledExecutorService refreshExecutor;
    private final Duration refreshBuffer;
    private final RESTCatalog restCatalog;
    private final CacheStatsMBean statsMBean;
    
    public TemporaryCredentialsCache(
            IcebergRestConfig config,
            RESTCatalog restCatalog,
            MBeanExporter exporter) {
        
        this.refreshBuffer = Duration.ofMinutes(5);  // Configurable
        this.restCatalog = restCatalog;
        
        this.cache = CacheBuilder.newBuilder()
            .maximumSize(config.getCredentialCacheSize())
            .expireAfterWrite(Duration.ofHours(1))  // Safety net
            .removalListener(this::onCredentialRemoved)
            .recordStats()
            .build(new CacheLoader<CredentialCacheKey, TemporaryCredential>() {
                @Override
                public TemporaryCredential load(CredentialCacheKey key) {
                    return fetchCredentialsFromRestCatalog(key);
                }
            });
        
        this.refreshExecutor = Executors.newScheduledThreadPool(1);
        this.statsMBean = new CacheStatsMBean(cache);
        exporter.export("presto.iceberg:name=TemporaryCredentialsCache", statsMBean);
        
        startProactiveRefresh();
    }
    
    public TemporaryCredential getCredentials(CredentialCacheKey key) {
        try {
            TemporaryCredential credential = cache.get(key);
            
            // Check if refresh is needed
            if (credential.needsRefresh(refreshBuffer)) {
                scheduleImmediateRefresh(key);
            }
            
            return credential;
        }
        catch (ExecutionException e) {
            throw new PrestoException(ICEBERG_CREDENTIAL_ERROR, 
                "Failed to obtain temporary credentials", e);
        }
    }
    
    private TemporaryCredential fetchCredentialsFromRestCatalog(CredentialCacheKey key) {
        // Call REST Catalog to obtain credentials
        // This will be implemented based on Iceberg REST Catalog spec
        // The catalog returns credentials in the table load response
        return callRestCatalogForCredentials(key);
    }
    
    private void startProactiveRefresh() {
        refreshExecutor.scheduleAtFixedRate(() -> {
            cache.asMap().forEach((key, credential) -> {
                if (credential.needsRefresh(refreshBuffer)) {
                    refreshCredential(key);
                }
            });
        }, 1, 1, TimeUnit.MINUTES);
    }
    
    private void refreshCredential(CredentialCacheKey key) {
        try {
            TemporaryCredential newCredential = fetchCredentialsFromRestCatalog(key);
            cache.put(key, newCredential);
        }
        catch (Exception e) {
            // Log error but don't fail - existing credential may still be valid
            log.warn("Failed to refresh credentials for key: " + key, e);
        }
    }
    
    private void scheduleImmediateRefresh(CredentialCacheKey key) {
        refreshExecutor.submit(() -> refreshCredential(key));
    }
    
    private void onCredentialRemoved(RemovalNotification<CredentialCacheKey, TemporaryCredential> notification) {
        log.info("Credential removed from cache: key={}, cause={}", 
            notification.getKey(), notification.getCause());
    }
    
    @PreDestroy
    public void shutdown() {
        refreshExecutor.shutdown();
    }
}
```

### 4. Configuration Properties

Add to `IcebergRestConfig.java`:

```java
private int credentialCacheSize = 10;
private Duration credentialRefreshBuffer = Duration.ofMinutes(5);
private boolean credentialCachingEnabled = true;

@Config("iceberg.rest.credential.cache-size")
@ConfigDescription("Maximum number of credential entries to cache")
public IcebergRestConfig setCredentialCacheSize(int size) {
    this.credentialCacheSize = size;
    return this;
}

@Config("iceberg.rest.credential.refresh-buffer")
@ConfigDescription("Time before expiration to trigger credential refresh")
public IcebergRestConfig setCredentialRefreshBuffer(Duration buffer) {
    this.credentialRefreshBuffer = buffer;
    return this;
}

@Config("iceberg.rest.credential.caching-enabled")
@ConfigDescription("Enable caching of temporary credentials")
public IcebergRestConfig setCredentialCachingEnabled(boolean enabled) {
    this.credentialCachingEnabled = enabled;
    return this;
}
```

### 5. Integration with IcebergRestCatalogFactory

Modify `IcebergRestCatalogFactory.java`:

```java
public class IcebergRestCatalogFactory extends IcebergNativeCatalogFactory {
    private final TemporaryCredentialsCache credentialsCache;
    
    @Inject
    public IcebergRestCatalogFactory(
            IcebergConfig config,
            IcebergRestConfig catalogConfig,
            IcebergCatalogName catalogName,
            S3ConfigurationUpdater s3ConfigurationUpdater,
            GcsConfigurationInitializer gcsConfigurationInitialize,
            AzureConfigurationInitializer azureConfigurationInitialize,
            NodeVersion nodeVersion,
            TemporaryCredentialsCache credentialsCache) {
        super(config, catalogName, s3ConfigurationUpdater, 
              gcsConfigurationInitialize, azureConfigurationInitialize);
        this.credentialsCache = requireNonNull(credentialsCache, "credentialsCache is null");
        // ... existing initialization
    }
    
    public TemporaryCredential getCredentialsForSession(ConnectorSession session) {
        CredentialCacheKey key = new CredentialCacheKey(
            catalogName,
            catalogConfig.getSessionType()
                .filter(type -> type.equals(USER))
                .map(type -> session.getUser())
        );
        return credentialsCache.getCredentials(key);
    }
}
```

### 6. Module Binding

Add to `IcebergRestCatalogModule.java`:

```java
@Singleton
@Provides
public TemporaryCredentialsCache createCredentialsCache(
        IcebergRestConfig config,
        RESTCatalog restCatalog,
        MBeanExporter exporter) {
    if (!config.isCredentialCachingEnabled()) {
        return new NoOpCredentialsCache();
    }
    return new TemporaryCredentialsCache(config, restCatalog, exporter);
}
```

## Concurrent Access Protection

### Strategy: Striped Locks

```java
public class TemporaryCredentialsCache {
    private final Striped<Lock> refreshLocks = Striped.lock(16);
    
    private void refreshCredential(CredentialCacheKey key) {
        Lock lock = refreshLocks.get(key);
        lock.lock();
        try {
            // Check if another thread already refreshed
            TemporaryCredential current = cache.getIfPresent(key);
            if (current != null && !current.needsRefresh(refreshBuffer)) {
                return;  // Already refreshed
            }
            
            TemporaryCredential newCredential = fetchCredentialsFromRestCatalog(key);
            cache.put(key, newCredential);
        }
        finally {
            lock.unlock();
        }
    }
}
```

## Error Handling Strategy

### 1. Refresh Failures

```java
private void refreshCredential(CredentialCacheKey key) {
    try {
        TemporaryCredential newCredential = fetchCredentialsFromRestCatalog(key);
        cache.put(key, newCredential);
    }
    catch (Exception e) {
        TemporaryCredential existing = cache.getIfPresent(key);
        if (existing != null && !existing.isExpired()) {
            // Log warning but continue using existing credential
            log.warn("Failed to refresh credentials, using existing until expiration", e);
        }
        else {
            // Existing credential expired or missing - this is critical
            log.error("Failed to refresh expired credentials", e);
            cache.invalidate(key);
            throw new PrestoException(ICEBERG_CREDENTIAL_ERROR, 
                "Unable to obtain valid credentials", e);
        }
    }
}
```

### 2. REST Catalog Unavailability

- Implement exponential backoff for retries
- Continue using cached credentials if still valid
- Fail queries only when credentials are actually expired

### 3. Credential Validation

```java
private TemporaryCredential fetchCredentialsFromRestCatalog(CredentialCacheKey key) {
    TemporaryCredential credential = callRestCatalogForCredentials(key);
    
    // Validate credential before caching
    if (credential.isExpired()) {
        throw new PrestoException(ICEBERG_CREDENTIAL_ERROR, 
            "REST Catalog returned already-expired credentials");
    }
    
    if (credential.getTimeUntilExpiration().compareTo(Duration.ofMinutes(1)) < 0) {
        log.warn("REST Catalog returned credentials expiring in less than 1 minute");
    }
    
    return credential;
}
```

## Testing Strategy

### Unit Tests

1. **Cache Behavior Tests**
   - Verify credential caching and retrieval
   - Test cache eviction policies
   - Validate concurrent access protection

2. **Refresh Logic Tests**
   - Test proactive refresh triggers
   - Verify refresh buffer calculations
   - Test refresh failure handling

3. **Expiration Tests**
   - Test expired credential detection
   - Verify refresh scheduling

### Integration Tests

1. **REST Catalog Integration**
   - Mock REST Catalog responses
   - Test credential fetch and parse
   - Verify error handling

2. **Multi-Session Tests**
   - Test USER session type isolation
   - Verify cache key generation

## Monitoring and Observability

### JMX Metrics

Expose via `CacheStatsMBean`:
- Cache hit rate
- Cache miss rate
- Eviction count
- Load success/failure count

### Custom Metrics

```java
@Managed
public long getActiveCredentials() {
    return cache.size();
}

@Managed
public long getCredentialsNearingExpiration() {
    return cache.asMap().values().stream()
        .filter(c -> c.needsRefresh(refreshBuffer))
        .count();
}

@Managed
public long getRefreshFailureCount() {
    return refreshFailureCounter.get();
}
```

### Logging

- INFO: Credential refresh events
- WARN: Refresh failures with valid fallback
- ERROR: Critical failures (expired credentials, no fallback)

## Future Enhancements

### 1. Per-Table Credentials

If REST Catalog supports table-scoped credentials:

```java
public class CredentialCacheKey {
    private final String catalogName;
    private final Optional<String> schemaName;
    private final Optional<String> tableName;
    private final Optional<String> userId;
}
```

### 2. Credential Pre-warming

Pre-fetch credentials for frequently accessed tables:

```java
public void prewarmCredentials(List<SchemaTableName> tables) {
    tables.forEach(table -> {
        CredentialCacheKey key = createKey(table);
        refreshExecutor.submit(() -> cache.get(key));
    });
}
```

### 3. Multi-Cloud Support

Extend to support Azure SAS tokens, GCS tokens:

```java
public interface TemporaryCredential {
    CredentialType getType();  // AWS, AZURE, GCS
    boolean isExpired();
    boolean needsRefresh(Duration buffer);
}
```

## Implementation Checklist

- [ ] Create `TemporaryCredential` class
- [ ] Create `CredentialCacheKey` class
- [ ] Implement `TemporaryCredentialsCache` class
- [ ] Add configuration properties to `IcebergRestConfig`
- [ ] Integrate with `IcebergRestCatalogFactory`
- [ ] Add module bindings in `IcebergRestCatalogModule`
- [ ] Implement REST Catalog credential fetch logic
- [ ] Add concurrent access protection
- [ ] Implement error handling and retry logic
- [ ] Add JMX metrics and monitoring
- [ ] Write unit tests
- [ ] Write integration tests
- [ ] Update documentation

## Open Questions

1. **REST Catalog API**: What is the exact API endpoint and response format for obtaining credentials?
2. **Credential Scope**: Does the REST Catalog return per-warehouse or per-table credentials?
3. **Worker Distribution**: How should credentials be distributed to workers? (Via TaskUpdateRequest or separate mechanism?)
4. **Credential Format**: What is the exact structure of credentials returned by the REST Catalog?
5. **Refresh Trigger**: Should refresh be purely time-based or also triggered by access patterns?

## References

- Iceberg REST Catalog Specification
- Presto Iceberg Connector Documentation
- Guava Cache Documentation
- AWS STS Temporary Credentials Documentation