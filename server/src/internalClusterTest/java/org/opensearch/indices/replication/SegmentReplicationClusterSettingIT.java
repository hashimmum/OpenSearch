/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.Arrays;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.indices.IndicesService.CLUSTER_SETTING_REPLICATION_TYPE;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationClusterSettingIT extends OpenSearchIntegTestCase {

    protected static final String INDEX_NAME = "test-idx-1";
    private static final String SYSTEM_INDEX_NAME = ".test-system-index";
    protected static final int SHARD_COUNT = 1;
    protected static final int REPLICA_COUNT = 1;

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL, "true").build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(CLUSTER_SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    public static class TestPlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(
                new SystemIndexDescriptor(SYSTEM_INDEX_NAME, "System index for [" + getTestClass().getName() + ']')
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(SegmentReplicationClusterSettingIT.TestPlugin.class, MockTransportService.TestPlugin.class);
    }

    public void testReplicationWithSegmentReplicationClusterSetting() throws Exception {

        boolean isSystemIndex = randomBoolean();
        String indexName = isSystemIndex ? SYSTEM_INDEX_NAME : INDEX_NAME;

        // Starting two nodes with primary and replica shards respectively.
        final String primaryNode = internalCluster().startNode();
        createIndex(indexName);
        ensureYellowAndNoInitializingShards(indexName);
        final String replicaNode = internalCluster().startNode();
        ensureGreen(indexName);

        final GetSettingsResponse response = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(INDEX_NAME).includeDefaults(true))
            .actionGet();
        if (isSystemIndex) {
            assertEquals(response.getSetting(INDEX_NAME, SETTING_REPLICATION_TYPE), ReplicationType.DOCUMENT.toString());
        } else {
            assertEquals(response.getSetting(INDEX_NAME, SETTING_REPLICATION_TYPE), ReplicationType.SEGMENT.toString());
        }
    }

    public void testIndexReplicationSettingOverridesSegRepClusterSetting() throws Exception {
        Settings settings = Settings.builder().put(CLUSTER_SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT).build();
        final String ANOTHER_INDEX = "test-index";

        // Starting two nodes with primary and replica shards respectively.
        final String primaryNode = internalCluster().startNode(settings);
        prepareCreate(
            INDEX_NAME,
            Settings.builder()
                // we want to override cluster replication setting by passing a index replication setting
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
        ).get();
        createIndex(ANOTHER_INDEX);
        ensureYellowAndNoInitializingShards(INDEX_NAME, ANOTHER_INDEX);
        final String replicaNode = internalCluster().startNode(settings);
        ensureGreen(INDEX_NAME, ANOTHER_INDEX);

        final GetSettingsResponse response = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(INDEX_NAME, "test-index").includeDefaults(true))
            .actionGet();
        assertEquals(response.getSetting(INDEX_NAME, SETTING_REPLICATION_TYPE), ReplicationType.DOCUMENT.toString());
        assertEquals(response.getSetting("test-index", SETTING_REPLICATION_TYPE), ReplicationType.SEGMENT.toString());
    }

    public void testIndexReplicationSettingOverridesDocRepClusterSetting() throws Exception {
        Settings settings = Settings.builder().put(CLUSTER_SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT).build();
        final String ANOTHER_INDEX = "test-index";
        final String primaryNode = internalCluster().startNode(settings);
        prepareCreate(
            INDEX_NAME,
            Settings.builder()
                // we want to override cluster replication setting by passing a index replication setting
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        ).get();
        createIndex(ANOTHER_INDEX);
        ensureYellowAndNoInitializingShards(INDEX_NAME, ANOTHER_INDEX);
        final String replicaNode = internalCluster().startNode(settings);
        ensureGreen(INDEX_NAME, ANOTHER_INDEX);

        final GetSettingsResponse response = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(INDEX_NAME, "test-index").includeDefaults(true))
            .actionGet();
        assertEquals(response.getSetting(INDEX_NAME, SETTING_REPLICATION_TYPE), ReplicationType.SEGMENT.toString());
        assertEquals(response.getSetting("test-index", SETTING_REPLICATION_TYPE), ReplicationType.DOCUMENT.toString());
    }

    public void testIndexReopenCloseWithReplicationStrategyClusterSetting() throws Exception {
        final String primaryNode = internalCluster().startNode();
        final String replicaNode = internalCluster().startNode();
        prepareCreate(
            INDEX_NAME,
            Settings.builder()
                // we want to override cluster replication setting by passing a index replication setting
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
        ).get();
        ensureGreen(INDEX_NAME);

        logger.info("--> Closing the index ");
        client().admin().indices().prepareClose(INDEX_NAME).get();

        logger.info("--> Opening the index");
        client().admin().indices().prepareOpen(INDEX_NAME).get();

        ensureGreen(INDEX_NAME);

        final GetSettingsResponse response = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(INDEX_NAME).includeDefaults(true))
            .actionGet();
        assertEquals(response.getSetting(INDEX_NAME, SETTING_REPLICATION_TYPE), ReplicationType.DOCUMENT.toString());
    }

    public void testHiddenIndicesWithReplicationStrategyClusterSetting() throws Exception {
        final String primaryNode = internalCluster().startNode();
        final String replicaNode = internalCluster().startNode();
        prepareCreate(
            INDEX_NAME,
            Settings.builder()
                // we want to set index as hidden
                .put("index.hidden", true)
        ).get();
        ensureGreen(INDEX_NAME);

        // Verify that document replication strategy is used for hidden indices.
        final GetSettingsResponse response = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(INDEX_NAME).includeDefaults(true))
            .actionGet();
        assertEquals(response.getSetting(INDEX_NAME, SETTING_REPLICATION_TYPE), ReplicationType.DOCUMENT.toString());
    }

}
