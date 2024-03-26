/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.core.common.io.stream.Writeable;

/**
 * Interface for access to any cache stats. Allows accessing stats by dimension values.
 * Stores an immutable snapshot of stats for a cache. The cache maintains its own live counters.
 */
public interface CacheStats extends Writeable {// TODO: also extends ToXContentFragment (in API PR)

    // Method to get all 5 values at once
    CounterSnapshot getTotalStats();

    // Methods to get total values.
    long getTotalHits();

    long getTotalMisses();

    long getTotalEvictions();

    long getTotalSizeInBytes();

    long getTotalEntries();
}
