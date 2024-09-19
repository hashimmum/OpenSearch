/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.DefaultRandomObject;
import org.opensearch.gateway.remote.RemoteClusterStateSettings;
import org.opensearch.gateway.remote.model.RemoteReadResult;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.toPositiveLongAtMost;

/**
 * An abstract class that provides a base implementation for managing remote entities in the remote store.
 */
public abstract class AbstractRemoteWritableEntityManager implements RemoteWritableEntityManager {
    /**
     * A map that stores the remote writable entity stores, keyed by the entity type.
     */
    protected final Map<String, RemoteWritableEntityStore> remoteWritableEntityStores = new HashMap<>();

    protected RemoteClusterStateSettings remoteClusterStateSettings = new RemoteClusterStateSettings(
        Settings.EMPTY,
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );

    protected final Random random = DefaultRandomObject.INSTANCE;

    /**
     * Retrieves the remote writable entity store for the given entity.
     *
     * @param entity the entity for which the store is requested
     * @return the remote writable entity store for the given entity
     * @throws IllegalArgumentException if the entity type is unknown
     */
    protected RemoteWritableEntityStore getStore(AbstractClusterMetadataWriteableBlobEntity entity) {
        RemoteWritableEntityStore remoteStore = remoteWritableEntityStores.get(entity.getType());
        if (remoteStore == null) {
            throw new IllegalArgumentException("Unknown entity type [" + entity.getType() + "]");
        }
        return remoteStore;
    }

    /**
     * Returns an ActionListener for handling the write operation for the specified component, remote object, and latched action listener.
     *
     * @param component the component for which the write operation is performed
     * @param remoteEntity the remote object to be written
     * @param listener the listener to be notified when the write operation completes
     * @return an ActionListener for handling the write operation
     */
    protected abstract ActionListener<Void> getWrappedWriteListener(
        String component,
        AbstractClusterMetadataWriteableBlobEntity remoteEntity,
        ActionListener<ClusterMetadataManifest.UploadedMetadata> listener
    );

    /**
     * Returns an ActionListener for handling the read operation for the specified component,
     * remote object, and latched action listener.
     *
     * @param component the component for which the read operation is performed
     * @param remoteEntity the remote object to be read
     * @param listener the listener to be notified when the read operation completes
     * @return an ActionListener for handling the read operation
     */
    protected abstract ActionListener<Object> getWrappedReadListener(
        String component,
        AbstractClusterMetadataWriteableBlobEntity remoteEntity,
        ActionListener<RemoteReadResult> listener
    );

    @Override
    public void writeAsync(
        String component,
        AbstractClusterMetadataWriteableBlobEntity entity,
        ActionListener<ClusterMetadataManifest.UploadedMetadata> listener
    ) {
        getStore(entity).writeAsync(entity, getWrappedWriteListener(component, entity, listener));
    }

    @Override
    public void readAsync(String component, AbstractClusterMetadataWriteableBlobEntity entity, ActionListener<RemoteReadResult> listener) {
        long maxDelayInMillis = remoteClusterStateSettings.getRemoteStateReadMaxJitter().getMillis();
        final long delayInMillis = toPositiveLongAtMost(random.nextLong(), maxDelayInMillis);
        getStore(entity).readAsyncWithDelay(delayInMillis, entity, getWrappedReadListener(component, entity, listener));
    }

}
