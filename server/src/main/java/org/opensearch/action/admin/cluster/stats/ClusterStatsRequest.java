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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A request to get cluster level stats.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterStatsRequest extends BaseNodesRequest<ClusterStatsRequest> {

    private final Set<String> requestedMetrics = new HashSet<>(SubMetrics.allSubMetrics());

    public ClusterStatsRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_2_16_0)) {
            useAggregatedNodeLevelResponses = in.readOptionalBoolean();
        }
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            requestedMetrics.clear();
            requestedMetrics.addAll(in.readStringList());
        }
    }

    private Boolean useAggregatedNodeLevelResponses = false;

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * based on all nodes will be returned.
     */
    public ClusterStatsRequest(String... nodesIds) {
        super(false, nodesIds);
    }

    public boolean useAggregatedNodeLevelResponses() {
        return useAggregatedNodeLevelResponses;
    }

    public void useAggregatedNodeLevelResponses(boolean useAggregatedNodeLevelResponses) {
        this.useAggregatedNodeLevelResponses = useAggregatedNodeLevelResponses;
    }

    /**
     * Get the names of requested metrics, excluding indices, which are
     * handled separately.
     */
    public Set<String> requestedMetrics() {
        return new HashSet<>(requestedMetrics);
    }

    /**
     * Add subMetric
     */
    public ClusterStatsRequest addMetric(String subMetric) {
        if (SubMetrics.allSubMetrics().contains(subMetric) == false) {
            throw new IllegalStateException("Used an illegal subMetric: " + subMetric);
        }
        requestedMetrics.add(subMetric);
        return this;
    }

    public void clearMetrics() {
        requestedMetrics.clear();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_16_0)) {
            out.writeOptionalBoolean(useAggregatedNodeLevelResponses);
        }
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeStringArray(requestedMetrics.toArray(new String[0]));
        }
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the cluster stats endpoint.
     */
    @PublicApi(since = "3.0.0")
    public enum Metric {
        INDICES("indices"),
        NODES("nodes");

        private String metricName;

        Metric(String name) {
            this.metricName = name;
        }

        public String metricName() {
            return this.metricName;
        }

        public Set<String> getSubMetrics() {
            return SubMetrics.allSubMetrics(this);
        }
    }

    /**
     * An enumeration of the "core" sections of sub metrics that may be requested
     * from the cluster stats endpoint.
     */
    public enum SubMetrics {
        SHARDS("shards", Metric.INDICES),
        DOCS("docs", Metric.INDICES),
        STORE("store", Metric.INDICES),
        FIELDDATA("fielddata", Metric.INDICES),
        QUERY_CACHE("query_cache", Metric.INDICES),
        COMPLETION("completion", Metric.INDICES),
        SEGMENTS("segments", Metric.INDICES),
        ANALYSIS("analysis", Metric.INDICES),
        MAPPINGS("mappings", Metric.INDICES),

        OS("os", Metric.NODES),
        PROCESS("process", Metric.NODES),
        JVM("jvm", Metric.NODES),
        FS("fs", Metric.NODES),
        PLUGINS("plugins", Metric.NODES),
        INGEST("ingest", Metric.NODES),
        NETWORK_TYPES("network_types", Metric.NODES),
        DISCOVERY_TYPES("discovery_types", Metric.NODES),
        PACKAGING_TYPES("packaging_types", Metric.NODES);

        private String metricName;

        private Metric metricType;

        SubMetrics(String name, Metric type) {
            this.metricName = name;
            this.metricType = type;
        }

        public String metricName() {
            return this.metricName;
        }

        public Metric metricType() {
            return this.metricType;
        }

        public boolean containedIn(Set<String> metricNames) {
            return metricNames.contains(this.metricName());
        }

        public static Set<String> allSubMetrics() {
            return Arrays.stream(values()).map(SubMetrics::metricName).collect(Collectors.toSet());
        }

        public static Set<String> allSubMetrics(Metric metricType) {
            return Arrays.stream(values())
                .filter(metric -> metricType.equals(metric.metricType()))
                .map(SubMetrics::metricName)
                .collect(Collectors.toSet());
        }

        public static boolean containsMetricType(Set<String> metricNames, Metric metricType) {
            return allSubMetrics(metricType).stream().anyMatch(metricNames::contains);
        }

    }
}
