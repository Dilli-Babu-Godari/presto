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
package com.facebook.presto.iceberg.rest;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.CacheStatsMBean;
import com.facebook.presto.spi.PrestoException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.Striped;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_CREDENTIAL_ERROR;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;

/**
 * Cache for temporary storage credentials obtained from Iceberg REST Catalog.
 * <p>
 * This cache manages the lifecycle of temporary credentials including:
 * - Lazy loading on first access
 * - Proactive refresh before expiration
 * - Concurrent access protection
 * - Automatic cleanup on expiration
 * - JMX monitoring
 */
public class TemporaryCredentialsCache
{
    private static final Logger log = Logger.get(TemporaryCredentialsCache.java);

    private final LoadingCache<CredentialCacheKey, TemporaryCredential> cache;
    private final ScheduledExecutorService refreshExecutor;
    private final Duration refreshBuffer;
    private final Function<CredentialCacheKey, TemporaryCredential> credentialFetcher;
    private final CacheStatsMBean statsMBean;
    private final Striped<Lock> refreshLocks;
    private final AtomicLong refreshSuccessCount;
    private final AtomicLong refreshFailureCount;
    private final boolean enabled;

    public TemporaryCredentialsCache(
            IcebergRestConfig config,
            Function<CredentialCacheKey, TemporaryCredential> credentialFetcher)
    {
        this.enabled = config.isCredentialCachingEnabled();
        this.credentialFetcher = requireNonNull(credentialFetcher, "credentialFetcher is null");
        this.refreshBuffer = config.getCredentialRefreshBuffer();
        this.refreshLocks = Striped.lock(16);
        this.refreshSuccessCount = new AtomicLong(0);
        this.refreshFailureCount = new AtomicLong(0);

        if (!enabled) {
            this.cache = null;
            this.refreshExecutor = null;
            this.statsMBean = null;
            log.info("Temporary credentials caching is disabled");
            return;
        }

        this.cache = CacheBuilder.newBuilder()
                .maximumSize(config.getCredentialCacheSize())
                .expireAfterWrite(Duration.ofHours(1))  // Safety net for stuck entries
                .removalListener(new CredentialRemovalListener())
                .recordStats()
                .build(new CredentialCacheLoader());

        this.statsMBean = new CacheStatsMBean(cache);
        this.refreshExecutor = Executors.newScheduledThreadPool(
                1,
                runnable -> {
                    Thread thread = new Thread(runnable, "iceberg-credential-refresh");
                    thread.setDaemon(true);
                    return thread;
                });

        startProactiveRefresh();
        log.info("Temporary credentials cache initialized with size=%d, refreshBuffer=%s",
                config.getCredentialCacheSize(), refreshBuffer);
    }

    /**
     * Get credentials for the given cache key.
     * If credentials are not cached, they will be fetched from the REST Catalog.
     * If credentials are nearing expiration, a refresh will be scheduled.
     *
     * @param key the cache key
     * @return the temporary credentials
     * @throws PrestoException if credentials cannot be obtained
     */
    public TemporaryCredential getCredentials(CredentialCacheKey key)
    {
        requireNonNull(key, "key is null");

        if (!enabled) {
            // When caching is disabled, always fetch fresh credentials
            return credentialFetcher.apply(key);
        }

        try {
            TemporaryCredential credential = cache.get(key);

            // Schedule refresh if credential is nearing expiration
            if (credential.needsRefresh(refreshBuffer)) {
                log.debug("Credential for key %s needs refresh, scheduling immediate refresh", key);
                scheduleImmediateRefresh(key);
            }

            return credential;
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw new PrestoException(ICEBERG_CREDENTIAL_ERROR,
                    "Failed to obtain temporary credentials for key: " + key, e.getCause());
        }
    }

    /**
     * Invalidate credentials for the given key.
     * This forces a fresh fetch on the next access.
     *
     * @param key the cache key
     */
    public void invalidate(CredentialCacheKey key)
    {
        if (enabled && cache != null) {
            cache.invalidate(key);
            log.info("Invalidated credentials for key: %s", key);
        }
    }

    /**
     * Invalidate all cached credentials.
     */
    public void invalidateAll()
    {
        if (enabled && cache != null) {
            cache.invalidateAll();
            log.info("Invalidated all cached credentials");
        }
    }

    /**
     * Start the background thread that proactively refreshes credentials
     * before they expire.
     */
    private void startProactiveRefresh()
    {
        refreshExecutor.scheduleAtFixedRate(
                this::refreshExpiringCredentials,
                1,  // Initial delay
                1,  // Period
                TimeUnit.MINUTES);
        log.info("Started proactive credential refresh with 1-minute interval");
    }

    /**
     * Scan all cached credentials and refresh those nearing expiration.
     */
    private void refreshExpiringCredentials()
    {
        try {
            cache.asMap().forEach((key, credential) -> {
                if (credential.needsRefresh(refreshBuffer)) {
                    log.debug("Proactively refreshing credential for key: %s", key);
                    refreshCredential(key);
                }
            });
        }
        catch (Exception e) {
            log.error(e, "Error during proactive credential refresh");
        }
    }

    /**
     * Schedule an immediate refresh for the given key.
     *
     * @param key the cache key
     */
    private void scheduleImmediateRefresh(CredentialCacheKey key)
    {
        refreshExecutor.submit(() -> refreshCredential(key));
    }

    /**
     * Refresh credentials for the given key.
     * Uses striped locks to prevent concurrent refreshes of the same key.
     *
     * @param key the cache key
     */
    private void refreshCredential(CredentialCacheKey key)
    {
        Lock lock = refreshLocks.get(key);
        lock.lock();
        try {
            // Check if another thread already refreshed
            TemporaryCredential current = cache.getIfPresent(key);
            if (current != null && !current.needsRefresh(refreshBuffer)) {
                log.debug("Credential for key %s already refreshed by another thread", key);
                return;
            }

            log.info("Refreshing credentials for key: %s", key);
            TemporaryCredential newCredential = fetchAndValidateCredential(key);
            cache.put(key, newCredential);
            refreshSuccessCount.incrementAndGet();
            log.info("Successfully refreshed credentials for key: %s, expires in %s",
                    key, newCredential.getTimeUntilExpiration());
        }
        catch (Exception e) {
            refreshFailureCount.incrementAndGet();
            handleRefreshFailure(key, e);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Fetch and validate credentials from the REST Catalog.
     *
     * @param key the cache key
     * @return validated credentials
     * @throws PrestoException if credentials are invalid
     */
    private TemporaryCredential fetchAndValidateCredential(CredentialCacheKey key)
    {
        TemporaryCredential credential = credentialFetcher.apply(key);

        // Validate credential before caching
        if (credential.isExpired()) {
            throw new PrestoException(ICEBERG_CREDENTIAL_ERROR,
                    "REST Catalog returned already-expired credentials for key: " + key);
        }

        Duration timeUntilExpiration = credential.getTimeUntilExpiration();
        if (timeUntilExpiration.compareTo(Duration.ofMinutes(1)) < 0) {
            log.warn("REST Catalog returned credentials expiring in less than 1 minute for key: %s", key);
        }

        return credential;
    }

    /**
     * Handle credential refresh failures.
     * If existing credentials are still valid, log a warning and continue.
     * If existing credentials are expired or missing, throw an exception.
     *
     * @param key the cache key
     * @param error the error that occurred
     */
    private void handleRefreshFailure(CredentialCacheKey key, Exception error)
    {
        TemporaryCredential existing = cache.getIfPresent(key);
        if (existing != null && existing.isValid()) {
            log.warn(error, "Failed to refresh credentials for key %s, continuing with existing credentials (expires in %s)",
                    key, existing.getTimeUntilExpiration());
        }
        else {
            log.error(error, "Failed to refresh expired or missing credentials for key: %s", key);
            cache.invalidate(key);
            throw new PrestoException(ICEBERG_CREDENTIAL_ERROR,
                    "Unable to obtain valid credentials for key: " + key, error);
        }
    }

    /**
     * Shutdown the cache and background refresh thread.
     */
    @PreDestroy
    public void shutdown()
    {
        if (refreshExecutor != null) {
            refreshExecutor.shutdown();
            try {
                if (!refreshExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    refreshExecutor.shutdownNow();
                }
            }
            catch (InterruptedException e) {
                refreshExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            log.info("Temporary credentials cache shutdown complete");
        }
    }

    // JMX Metrics

    @Managed
    @Nested
    public CacheStatsMBean getCacheStats()
    {
        return statsMBean;
    }

    @Managed
    public long getActiveCredentials()
    {
        return enabled && cache != null ? cache.size() : 0;
    }

    @Managed
    public long getCredentialsNearingExpiration()
    {
        if (!enabled || cache == null) {
            return 0;
        }
        return cache.asMap().values().stream()
                .filter(c -> c.needsRefresh(refreshBuffer))
                .count();
    }

    @Managed
    public long getRefreshSuccessCount()
    {
        return refreshSuccessCount.get();
    }

    @Managed
    public long getRefreshFailureCount()
    {
        return refreshFailureCount.get();
    }

    @Managed
    public boolean isEnabled()
    {
        return enabled;
    }

    /**
     * Cache loader that fetches credentials from the REST Catalog.
     */
    private class CredentialCacheLoader
            extends CacheLoader<CredentialCacheKey, TemporaryCredential>
    {
        @Override
        public TemporaryCredential load(CredentialCacheKey key)
        {
            log.info("Loading credentials for key: %s", key);
            return fetchAndValidateCredential(key);
        }
    }

    /**
     * Listener for credential removal events.
     */
    private class CredentialRemovalListener
            implements RemovalListener<CredentialCacheKey, TemporaryCredential>
    {
        @Override
        public void onRemoval(RemovalNotification<CredentialCacheKey, TemporaryCredential> notification)
        {
            log.info("Credential removed from cache: key=%s, cause=%s",
                    notification.getKey(), notification.getCause());
        }
    }
}
