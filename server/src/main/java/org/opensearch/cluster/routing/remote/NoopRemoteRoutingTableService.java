/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.remote;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.gateway.remote.ClusterMetadataManifest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class NoopRemoteRoutingTableService extends AbstractLifecycleComponent implements RemoteRoutingTableService {

    @Override
    public List<IndexRoutingTable> getIndicesRouting(RoutingTable routingTable) {
        return List.of();
    }

    @Override
    public DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> getIndicesRoutingMapDiff(
        RoutingTable before,
        RoutingTable after
    ) {
        return DiffableUtils.diff(Map.of(), Map.of(), DiffableUtils.getStringKeySerializer(), CUSTOM_ROUTING_TABLE_VALUE_SERIALIZER);
    }

    @Override
    public CheckedRunnable<IOException> getIndexRoutingAsyncAction(
        ClusterState clusterState,
        IndexRoutingTable indexRouting,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener,
        BlobPath clusterBasePath
    ) {
        // noop
        return () -> {};
    }

    @Override
    public List<ClusterMetadataManifest.UploadedIndexMetadata> getAllUploadedIndicesRouting(
        ClusterMetadataManifest previousManifest,
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingUploaded,
        List<String> indicesRoutingToDelete
    ) {
        return List.of();
    }

    @Override
    protected void doStart() {
        // noop
    }

    @Override
    protected void doStop() {
        // noop
    }

    @Override
    protected void doClose() throws IOException {
        // noop
    }
}
