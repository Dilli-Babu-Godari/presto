# Iceberg REST Catalog Response Format

## Overview

This document describes the actual response format from the Iceberg REST Catalog when loading a table, which includes temporary credentials in the `config` section.

## Sample Response

```json
{
    "metadata-location": "s3://bucket/path/metadata/00002-xxx.metadata.json",
    "metadata": {
        "format-version": 2,
        "table-uuid": "dd906c97-54fa-46e2-8729-e78db60e888c",
        "location": "s3://bucket/path/",
        ...
    },
    "config": {
        "expires-at-ms": "1779829898000",
        "s3.access-key-id": "ASIAQFLZD5O6VBITYVPZ",
        "s3.session-token": "IQoJb3JpZ2luX2VjEL3//...",
        "s3.secret-access-key": "k2nXHgRrGf0tfg5R6qvx1uKHpS+D7pVJRQHvwf8i",
        "client.region": "us-east-2"
    }
}
```

## Credential Fields in `config` Section

| Field | Description | Example |
|-------|-------------|---------|
| `expires-at-ms` | Expiration timestamp in milliseconds (epoch) | `"1779829898000"` |
| `s3.access-key-id` | AWS temporary access key ID | `"ASIAQFLZD5O6VBITYVPZ"` |
| `s3.secret-access-key` | AWS temporary secret access key | `"k2nXHgRrGf0tfg5R6qvx1uKHpS+D7pVJRQHvwf8i"` |
| `s3.session-token` | AWS session token for temporary credentials | `"IQoJb3JpZ2luX2VjEL3//..."` |
| `client.region` | AWS region for the S3 bucket | `"us-east-2"` |

## Key Observations

### 1. Credentials are Per-Table

The credentials are returned as part of the table load response, suggesting they are **per-table** rather than per-catalog or per-warehouse.

**Implication**: The cache key strategy should be updated to support per-table caching if needed, though per-catalog caching can still work if all tables in a catalog share the same credentials.

### 2. Expiration Format

The expiration is provided as `expires-at-ms` (milliseconds since epoch), not as a duration.

**Conversion**:
```java
long expiresAtMs = Long.parseLong(config.get("expires-at-ms"));
Instant expirationTime = Instant.ofEpochMilli(expiresAtMs);
```

### 3. AWS-Specific Format

The current response shows AWS S3 credentials with the prefix `s3.`. Other cloud providers would likely use different prefixes:
- Azure: `adls.` or `azure.`
- GCS: `gcs.` or `gs.`

### 4. Storage Location

The storage location can be derived from the `metadata.location` field:
```
"location": "s3://deltasharebkt/schema_shuang/__unitystorage/..."
```

## Parsing Logic

### Extract Credentials from Response

```java
public TemporaryCredential parseCredentialsFromResponse(Map<String, Object> response) {
    Map<String, String> config = (Map<String, String>) response.get("config");
    Map<String, Object> metadata = (Map<String, Object>) response.get("metadata");
    
    if (config == null) {
        throw new PrestoException(ICEBERG_CREDENTIAL_ERROR, 
            "No config section in REST Catalog response");
    }
    
    // Extract expiration
    String expiresAtMs = config.get("expires-at-ms");
    if (expiresAtMs == null) {
        throw new PrestoException(ICEBERG_CREDENTIAL_ERROR, 
            "No expires-at-ms in REST Catalog config");
    }
    Instant expirationTime = Instant.ofEpochMilli(Long.parseLong(expiresAtMs));
    
    // Extract AWS credentials
    String accessKey = config.get("s3.access-key-id");
    String secretKey = config.get("s3.secret-access-key");
    String sessionToken = config.get("s3.session-token");
    
    if (accessKey == null || secretKey == null || sessionToken == null) {
        throw new PrestoException(ICEBERG_CREDENTIAL_ERROR, 
            "Missing required S3 credentials in REST Catalog config");
    }
    
    // Extract storage location
    String storageLocation = (String) metadata.get("location");
    
    return new TemporaryCredential(
        accessKey,
        secretKey,
        sessionToken,
        expirationTime,
        storageLocation
    );
}
```

### Handle Multi-Cloud Support

```java
public TemporaryCredential parseCredentials(Map<String, String> config, String storageLocation) {
    // Detect cloud provider from config keys
    if (config.containsKey("s3.access-key-id")) {
        return parseAwsCredentials(config, storageLocation);
    }
    else if (config.containsKey("adls.sas-token")) {
        return parseAzureCredentials(config, storageLocation);
    }
    else if (config.containsKey("gcs.oauth2.token")) {
        return parseGcsCredentials(config, storageLocation);
    }
    else {
        throw new PrestoException(ICEBERG_CREDENTIAL_ERROR, 
            "Unknown credential type in REST Catalog config");
    }
}
```

## Cache Key Strategy Update

Given that credentials are per-table, the cache key should be updated:

### Option 1: Per-Table Caching (Most Accurate)

```java
public class CredentialCacheKey {
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final Optional<String> userId;
}
```

**Pros**:
- Most accurate - matches REST Catalog behavior
- Different tables can have different credentials

**Cons**:
- More cache entries
- Higher memory usage

### Option 2: Per-Catalog Caching (Current Implementation)

```java
public class CredentialCacheKey {
    private final String catalogName;
    private final Optional<String> userId;
}
```

**Pros**:
- Fewer cache entries
- Lower memory usage
- Works if all tables share credentials

**Cons**:
- May not work if different tables have different credentials
- Credentials from first table used for all tables

### Recommendation

**Start with per-catalog caching** (current implementation) and add per-table support later if needed. Most deployments likely use the same credentials for all tables in a catalog.

## Integration Example

```java
public class IcebergRestCatalogFactory {
    private final TemporaryCredentialsCache credentialsCache;
    
    public TemporaryCredential getCredentialsForTable(
            ConnectorSession session,
            SchemaTableName table) {
        
        // Create cache key
        CredentialCacheKey key = new CredentialCacheKey(
            catalogName,
            getUserId(session)
        );
        
        // Get or fetch credentials
        return credentialsCache.getCredentials(key);
    }
    
    private TemporaryCredential fetchFromRestCatalog(CredentialCacheKey key) {
        // Call REST Catalog to load table
        // This returns the full response including config section
        Map<String, Object> response = restCatalog.loadTable(...);
        
        // Parse credentials from config section
        return parseCredentialsFromResponse(response);
    }
}
```

## Expiration Calculation

Given the sample response:
```
"expires-at-ms": "1779829898000"
```

This is approximately **2.5 hours** from the request time (assuming request was around 1779820612884).

**Refresh Strategy**:
- With 5-minute buffer: Refresh at `1779829598000` (5 minutes before expiration)
- This gives plenty of time for refresh failures and retries

## Testing Considerations

### Mock Response for Testing

```java
Map<String, Object> mockResponse = ImmutableMap.of(
    "metadata-location", "s3://bucket/path/metadata.json",
    "metadata", ImmutableMap.of(
        "location", "s3://bucket/path/"
    ),
    "config", ImmutableMap.of(
        "expires-at-ms", String.valueOf(Instant.now().plus(Duration.ofHours(1)).toEpochMilli()),
        "s3.access-key-id", "ASIAQFLZD5O6VBITYVPZ",
        "s3.secret-access-key", "test-secret-key",
        "s3.session-token", "test-session-token",
        "client.region", "us-east-2"
    )
);
```

### Test Cases

1. **Valid Response** - Parse credentials successfully
2. **Missing Config** - Throw appropriate exception
3. **Missing Expiration** - Throw appropriate exception
4. **Missing Credentials** - Throw appropriate exception
5. **Expired Credentials** - Detect and reject
6. **Near-Expiration** - Trigger refresh
7. **Multi-Cloud** - Parse Azure/GCS credentials

## Security Considerations

### 1. Credential Logging

**Never log actual credential values**. The `TemporaryCredential.toString()` method already masks sensitive fields:

```java
@Override
public String toString() {
    return "TemporaryCredential{" +
            "accessKey=***" +
            ", secretKey=***" +
            ", sessionToken=***" +
            ", expirationTime=" + expirationTime +
            ", storageLocation='" + storageLocation + '\'' +
            '}';
}
```

### 2. Credential Storage

Credentials are stored in-memory only (Guava cache). They are never persisted to disk.

### 3. Credential Transmission

When distributing credentials to workers, ensure they are transmitted securely (e.g., over encrypted channels).

## Next Steps

1. **Implement Parser** - Create `RestCatalogCredentialParser` class
2. **Update Cache Loader** - Use parser in credential fetcher
3. **Add Multi-Cloud Support** - Extend parser for Azure/GCS
4. **Add Tests** - Unit tests for parser and cache
5. **Consider Per-Table Caching** - If needed based on deployment patterns