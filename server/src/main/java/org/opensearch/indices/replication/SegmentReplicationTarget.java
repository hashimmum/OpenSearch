/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.common.ReplicationTarget;

import java.io.IOException;

/**
 * Represents the target of a replication event.
 *
 * @opensearch.internal
 */
public class SegmentReplicationTarget extends ReplicationTarget {

    private final ReplicationCheckpoint checkpoint;
    private final SegmentReplicationSource source;
    private final SegmentReplicationState state;

    public SegmentReplicationTarget(
        ReplicationCheckpoint checkpoint,
        IndexShard indexShard,
        SegmentReplicationSource source,
        SegmentReplicationTargetService.SegmentReplicationListener listener
    ) {
        super("replication_target", indexShard, new ReplicationLuceneIndex(), listener);
        this.checkpoint = checkpoint;
        this.source = source;
        this.state = new SegmentReplicationState();
    }

    @Override
    protected void closeInternal() {

    }

    @Override
    protected String getPrefix() {
        return null;
    }

    @Override
    protected void onDone() {

    }

    @Override
    protected void onCancel(String reason) {

    }

    @Override
    public ReplicationState state() {
        return null;
    }

    @Override
    public ReplicationTarget retryCopy() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }

    @Override
    public void notifyListener(OpenSearchException e, boolean sendShardFailure) {
        listener.onFailure(state(), e, sendShardFailure);
    }

    @Override
    public boolean reset(CancellableThreads newTargetCancellableThreads) throws IOException {
        return false;
    }

    @Override
    public void writeFileChunk(
        StoreFileMetadata metadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {

    }

    public void startReplication() {
        // TODO
    }
}
