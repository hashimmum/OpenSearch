/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * A class containing the 5 live metrics tracked by a CacheStats object. Mutable.
 */
public class CacheStatsResponse {
    public CounterMetric hits;
    public CounterMetric misses;
    public CounterMetric evictions;
    public CounterMetric sizeInBytes;
    public CounterMetric entries;

    public CacheStatsResponse(long hits, long misses, long evictions, long sizeInBytes, long entries) {
        this.hits = new CounterMetric();
        this.hits.inc(hits);
        this.misses = new CounterMetric();
        this.misses.inc(misses);
        this.evictions = new CounterMetric();
        this.evictions.inc(evictions);
        this.sizeInBytes = new CounterMetric();
        this.sizeInBytes.inc(sizeInBytes);
        this.entries = new CounterMetric();
        this.entries.inc(entries);
    }

    public CacheStatsResponse() {
        this(0, 0, 0, 0, 0);
    }

    private synchronized void internalAdd(long otherHits, long otherMisses, long otherEvictions, long otherSizeInBytes, long otherEntries) {
        this.hits.inc(otherHits);
        this.misses.inc(otherMisses);
        this.evictions.inc(otherEvictions);
        this.sizeInBytes.inc(otherSizeInBytes);
        this.entries.inc(otherEntries);
    }

    public void add(CacheStatsResponse other) {
        if (other == null) {
            return;
        }
        internalAdd(other.hits.count(), other.misses.count(), other.evictions.count(), other.sizeInBytes.count(), other.entries.count());
    }

    public void add(CacheStatsResponse.Snapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        internalAdd(snapshot.hits, snapshot.misses, snapshot.evictions, snapshot.sizeInBytes, snapshot.entries);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o.getClass() != CacheStatsResponse.class) {
            return false;
        }
        CacheStatsResponse other = (CacheStatsResponse) o;
        return (hits.count() == other.hits.count())
            && (misses.count() == other.misses.count())
            && (evictions.count() == other.evictions.count())
            && (sizeInBytes.count() == other.sizeInBytes.count())
            && (entries.count() == other.entries.count());
    }

    @Override
    public int hashCode() {
        return Objects.hash(hits.count(), misses.count(), evictions.count(), sizeInBytes.count(), entries.count());
    }

    public long getHits() {
        return hits.count();
    }

    public long getMisses() {
        return misses.count();
    }

    public long getEvictions() {
        return evictions.count();
    }

    public long getSizeInBytes() {
        return sizeInBytes.count();
    }

    public long getEntries() {
        return entries.count();
    }

    public Snapshot snapshot() {
        return new Snapshot(hits.count(), misses.count(), evictions.count(), sizeInBytes.count(), entries.count());
    }

    /**
     * An immutable snapshot of CacheStatsResponse.
     */
    public static class Snapshot implements Writeable { // TODO: Make this extend ToXContent (in API PR)
        private final long hits;
        private final long misses;
        private final long evictions;
        private final long sizeInBytes;
        private final long entries;

        public Snapshot(long hits, long misses, long evictions, long sizeInBytes, long entries) {
            this.hits = hits;
            this.misses = misses;
            this.evictions = evictions;
            this.sizeInBytes = sizeInBytes;
            this.entries = entries;
        }

        public Snapshot(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        public long getHits() {
            return hits;
        }

        public long getMisses() {
            return misses;
        }

        public long getEvictions() {
            return evictions;
        }

        public long getSizeInBytes() {
            return sizeInBytes;
        }

        public long getEntries() {
            return entries;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(hits);
            out.writeVLong(misses);
            out.writeVLong(evictions);
            out.writeVLong(sizeInBytes);
            out.writeVLong(entries);
        }

        public Snapshot add(Snapshot other) {
            return new Snapshot(
                hits + other.hits,
                misses + other.misses,
                evictions + other.evictions,
                sizeInBytes + other.sizeInBytes,
                entries + other.entries
            );
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            if (o.getClass() != CacheStatsResponse.Snapshot.class) {
                return false;
            }
            CacheStatsResponse.Snapshot other = (CacheStatsResponse.Snapshot) o;
            return (hits == other.hits)
                && (misses == other.misses)
                && (evictions == other.evictions)
                && (sizeInBytes == other.sizeInBytes)
                && (entries == other.entries);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hits, misses, evictions, sizeInBytes, entries);
        }
    }
}
