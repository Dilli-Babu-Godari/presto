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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents temporary storage credentials obtained from an Iceberg REST Catalog.
 * These credentials are short-lived and include an expiration time.
 */
public class TemporaryCredential
{
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
    private final Instant expirationTime;
    private final String storageLocation;

    @JsonCreator
    public TemporaryCredential(
            @JsonProperty("accessKey") String accessKey,
            @JsonProperty("secretKey") String secretKey,
            @JsonProperty("sessionToken") String sessionToken,
            @JsonProperty("expirationTime") Instant expirationTime,
            @JsonProperty("storageLocation") String storageLocation)
    {
        this.accessKey = requireNonNull(accessKey, "accessKey is null");
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
        this.sessionToken = requireNonNull(sessionToken, "sessionToken is null");
        this.expirationTime = requireNonNull(expirationTime, "expirationTime is null");
        this.storageLocation = requireNonNull(storageLocation, "storageLocation is null");
    }

    @JsonProperty
    public String getAccessKey()
    {
        return accessKey;
    }

    @JsonProperty
    public String getSecretKey()
    {
        return secretKey;
    }

    @JsonProperty
    public String getSessionToken()
    {
        return sessionToken;
    }

    @JsonProperty
    public Instant getExpirationTime()
    {
        return expirationTime;
    }

    @JsonProperty
    public String getStorageLocation()
    {
        return storageLocation;
    }

    /**
     * Check if the credential has expired.
     *
     * @return true if the credential is expired, false otherwise
     */
    public boolean isExpired()
    {
        return Instant.now().isAfter(expirationTime);
    }

    /**
     * Check if the credential needs to be refreshed based on a buffer time.
     * This allows proactive refresh before actual expiration.
     *
     * @param bufferTime the time buffer before expiration
     * @return true if the credential should be refreshed, false otherwise
     */
    public boolean needsRefresh(Duration bufferTime)
    {
        requireNonNull(bufferTime, "bufferTime is null");
        return Instant.now().plus(bufferTime).isAfter(expirationTime);
    }

    /**
     * Get the duration until the credential expires.
     *
     * @return the duration until expiration
     */
    public Duration getTimeUntilExpiration()
    {
        return Duration.between(Instant.now(), expirationTime);
    }

    /**
     * Check if the credential is still valid (not expired).
     *
     * @return true if the credential is valid, false otherwise
     */
    public boolean isValid()
    {
        return !isExpired();
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
        TemporaryCredential that = (TemporaryCredential) o;
        return Objects.equals(accessKey, that.accessKey) &&
                Objects.equals(secretKey, that.secretKey) &&
                Objects.equals(sessionToken, that.sessionToken) &&
                Objects.equals(expirationTime, that.expirationTime) &&
                Objects.equals(storageLocation, that.storageLocation);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(accessKey, secretKey, sessionToken, expirationTime, storageLocation);
    }

    @Override
    public String toString()
    {
        return "TemporaryCredential{" +
                "accessKey=***" +
                ", secretKey=***" +
                ", sessionToken=***" +
                ", expirationTime=" + expirationTime +
                ", storageLocation='" + storageLocation + '\'' +
                '}';
    }
}
