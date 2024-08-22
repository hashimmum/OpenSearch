/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugin.wlm.action.CreateQueryGroupAction;
import org.opensearch.plugin.wlm.action.GetQueryGroupAction;
import org.opensearch.plugin.wlm.action.TransportCreateQueryGroupAction;
import org.opensearch.plugin.wlm.action.TransportGetQueryGroupAction;
import org.opensearch.plugin.wlm.rest.RestCreateQueryGroupAction;
import org.opensearch.plugin.wlm.rest.RestGetQueryGroupAction;
import org.opensearch.plugin.wlm.service.QueryGroupPersistenceService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

import java.util.List;
import java.util.function.Supplier;

/**
 * Plugin class for WorkloadManagement
 */
public class WorkloadManagementPlugin extends Plugin implements ActionPlugin {

    /**
     * Default constructor
     */
    public WorkloadManagementPlugin() {}

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionPlugin.ActionHandler<>(CreateQueryGroupAction.INSTANCE, TransportCreateQueryGroupAction.class),
            new ActionPlugin.ActionHandler<>(GetQueryGroupAction.INSTANCE, TransportGetQueryGroupAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestCreateQueryGroupAction(), new RestGetQueryGroupAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(QueryGroupPersistenceService.MAX_QUERY_GROUP_COUNT);
    }
}
