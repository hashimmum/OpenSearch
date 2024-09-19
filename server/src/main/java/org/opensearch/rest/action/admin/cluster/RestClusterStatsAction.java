/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest.SubMetrics;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions.NodesResponseRestListener;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to get cluster stats
 *
 * @opensearch.api
 */
public class RestClusterStatsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_cluster/stats"),
                new Route(GET, "/_cluster/stats/nodes/{nodeId}"),
                new Route(GET, "/_cluster/stats/metrics/{metric}"),
                new Route(GET, "/_cluster/stats/metrics/{metric}/{sub_metric}"),
                new Route(GET, "/_cluster/stats/nodes/{nodeId}/metrics/{metric}"),
                new Route(GET, "/_cluster/stats/nodes/{nodeId}/metrics/{metric}/{sub_metric}")
            )
        );
    }

    static final Map<String, Set<String>> METRIC_TO_SUB_METRICS_MAP;

    static {
        Map<String, Set<String>> metricMap = new HashMap<>();
        for (ClusterStatsRequest.Metric metric : ClusterStatsRequest.Metric.values()) {
            metricMap.put(metric.metricName(), metric.getSubMetrics());
        }
        METRIC_TO_SUB_METRICS_MAP = Collections.unmodifiableMap(metricMap);
    }

    static final Map<String, Consumer<ClusterStatsRequest>> SUB_METRIC_REQUEST_CONSUMER_MAP;

    static {
        Map<String, Consumer<ClusterStatsRequest>> subMetricMap = new HashMap<>();
        for (SubMetrics subMetric : SubMetrics.values()) {
            subMetricMap.put(subMetric.metricName(), request -> request.addMetric(subMetric.metricName()));
        }
        SUB_METRIC_REQUEST_CONSUMER_MAP = Collections.unmodifiableMap(subMetricMap);
    }

    @Override
    public String getName() {
        return "cluster_stats_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] nodeIds = request.paramAsStringArray("nodeId", null);
        Set<String> metrics = Strings.tokenizeByCommaToSet(request.param("metric", "_all"));
        Set<String> subMetrics = Strings.tokenizeByCommaToSet(request.param("sub_metric", "_all"));

        ClusterStatsRequest clusterStatsRequest = new ClusterStatsRequest().nodesIds(nodeIds);
        clusterStatsRequest.timeout(request.param("timeout"));
        clusterStatsRequest.useAggregatedNodeLevelResponses(true);

        if (metrics.size() > 1 && metrics.contains("_all")) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "request [%s] contains _all and individual metrics [%s]",
                    request.path(),
                    request.param("metric")
                )
            );
        } else if (subMetrics.size() > 1 && subMetrics.contains("_all")) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "request [%s] contains _all and individual sub metrics [%s]",
                    request.path(),
                    request.param("sub_metric")
                )
            );
        } else {
            clusterStatsRequest.clearMetrics();
            final Set<String> eligibleSubMetrics = new HashSet<>();
            final Set<String> invalidMetrics = new TreeSet<>();
            if (metrics.contains("_all")) {
                eligibleSubMetrics.addAll(SubMetrics.allSubMetrics());
            } else {
                for (String metric : metrics) {
                    Set<String> metricTypeSubMetrics = METRIC_TO_SUB_METRICS_MAP.get(metric);
                    if (metricTypeSubMetrics != null) {
                        eligibleSubMetrics.addAll(metricTypeSubMetrics);
                    } else {
                        invalidMetrics.add(metric);
                    }
                }
            }
            if (!invalidMetrics.isEmpty()) {
                throw new IllegalArgumentException(unrecognized(request, invalidMetrics, METRIC_TO_SUB_METRICS_MAP.keySet(), "metric"));
            }

            final Set<String> subMetricsRequested = new HashSet<>();
            final Set<String> invalidSubMetrics = new TreeSet<>();
            if (subMetrics.contains("_all")) {
                subMetricsRequested.addAll(eligibleSubMetrics);
            } else {
                for (String subMetric : subMetrics) {
                    if (eligibleSubMetrics.contains(subMetric)) {
                        subMetricsRequested.add(subMetric);
                    } else {
                        invalidSubMetrics.add(subMetric);
                    }
                }
            }

            if (!invalidSubMetrics.isEmpty()) {
                throw new IllegalArgumentException(
                    unrecognized(request, invalidSubMetrics, SUB_METRIC_REQUEST_CONSUMER_MAP.keySet(), "sub_metric")
                );
            }

            for (String subMetric : subMetricsRequested) {
                SUB_METRIC_REQUEST_CONSUMER_MAP.get(subMetric).accept(clusterStatsRequest);
            }

        }

        return channel -> client.admin().cluster().clusterStats(clusterStatsRequest, new NodesResponseRestListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
