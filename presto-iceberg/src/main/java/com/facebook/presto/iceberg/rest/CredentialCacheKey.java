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

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Cache key for temporary credentials.
 * Supports per-catalog caching with optional user-level isolation.
 */
public class CredentialCacheKey
{
    private final String catalogName;
    private final Optional<String> userId;

    public CredentialCacheKey(String catalogName, Optional<String> userId)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.userId = requireNonNull(userId, "userId is null");
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public Optional<String> getUserId()
    {
        return userId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CredentialCacheKey that = (CredentialCacheKey) o;
        return Objects.equals(catalogName, that.catalogName) &&
                Objects.equals(userId, that.userId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, userId);
    }

    @Override
    public String toString()
    {
        return "CredentialCacheKey{" +
                "catalogName='" + catalogName + '\'' +
                ", userId=" + userId +
                '}';
    }
}
