/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.wlm.stats.QueryGroupState;
import org.opensearch.wlm.stats.QueryGroupStats;
import org.opensearch.wlm.stats.QueryGroupStats.QueryGroupStatsHolder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * As of now this is a stub and main implementation PR will be raised soon.Coming PR will collate these changes with core QueryGroupService changes
 * @opensearch.experimental
 */
public class QueryGroupService {
    // This map does not need to be concurrent since we will process the cluster state change serially and update
    // this map with new additions and deletions of entries. QueryGroupState is thread safe
    private final Map<String, QueryGroupState> queryGroupStateMap;
    private final DiscoveryNode discoveryNode;
    private final ClusterService clusterService;

    @Inject
    public QueryGroupService(DiscoveryNode discoveryNode, ClusterService clusterService) {
        this(discoveryNode, clusterService, new HashMap<>());
    }

    @Inject
    public QueryGroupService(DiscoveryNode discoveryNode, ClusterService clusterService, Map<String, QueryGroupState> queryGroupStateMap) {
        this.discoveryNode = discoveryNode;
        this.clusterService = clusterService;
        this.queryGroupStateMap = queryGroupStateMap;
    }

    /**
     * updates the failure stats for the query group
     * @param queryGroupId query group identifier
     */
    public void incrementFailuresFor(final String queryGroupId) {
        QueryGroupState queryGroupState = queryGroupStateMap.get(queryGroupId);
        // This can happen if the request failed for a deleted query group
        // or new queryGroup is being created and has not been acknowledged yet
        if (queryGroupState == null) {
            return;
        }
        queryGroupState.failures.inc();
    }

    /**
     * @return node level query group stats
     */
    public QueryGroupStats nodeStats(Set<String> queryGroupIds, Boolean requestedBreached) {
        final Map<String, QueryGroupStatsHolder> statsHolderMap = new HashMap<>();

        queryGroupStateMap.forEach((queryGroupId, currentState) -> {
            boolean shouldInclude = (queryGroupIds.size() == 1 && queryGroupIds.contains("_all")) || queryGroupIds.contains(queryGroupId);

            if (shouldInclude) {
                if (requestedBreached == null || requestedBreached == resourceLimitBreached(queryGroupId, currentState)) {
                    statsHolderMap.put(queryGroupId, QueryGroupStatsHolder.from(currentState));
                }
            }
        });

        return new QueryGroupStats(discoveryNode, statsHolderMap);
    }

    /**
     * @return if the QueryGroup breaches any resource limit based on the LastRecordedUsage
     */
    public boolean resourceLimitBreached(String id, QueryGroupState currentState) {
        QueryGroup queryGroup = clusterService.state().metadata().queryGroups().get(id);

        return currentState.getResourceState()
            .entrySet()
            .stream()
            .anyMatch(
                entry -> entry.getValue().getLastRecordedUsage() > queryGroup.getMutableQueryGroupFragment()
                    .getResourceLimits()
                    .getOrDefault(entry.getKey(), 0.0)
            );
    }

    /**
     *
     * @param queryGroupId query group identifier
     */
    public void rejectIfNeeded(String queryGroupId) {
        if (queryGroupId == null) return;
        boolean reject = false;
        final StringBuilder reason = new StringBuilder();
        // TODO: At this point this is dummy and we need to decide whether to cancel the request based on last
        // reported resource usage for the queryGroup. We also need to increment the rejection count here for the
        // query group
        if (reject) {
            throw new OpenSearchRejectedExecutionException("QueryGroup " + queryGroupId + " is already contended." + reason.toString());
        }
    }
}
