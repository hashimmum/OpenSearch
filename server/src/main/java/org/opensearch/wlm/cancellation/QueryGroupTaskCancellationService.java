/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.cancellation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.wlm.MutableQueryGroupFragment.ResiliencyMode;
import org.opensearch.wlm.QueryGroupLevelResourceUsageView;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.stats.QueryGroupState;
import org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService.TRACKED_RESOURCES;

/**
 * Manages the cancellation of tasks enforced by QueryGroup thresholds on resource usage criteria.
 * This class utilizes a strategy pattern through {@link MaximumResourceTaskSelectionStrategy} to identify tasks that exceed
 * predefined resource usage limits and are therefore eligible for cancellation.
 *
 * <p>The cancellation process is initiated by evaluating the resource usage of each QueryGroup against its
 * resource limits. Tasks that contribute to exceeding these limits are selected for cancellation based on the
 * implemented task selection strategy.</p>
 *
 * <p>Instances of this class are configured with a map linking QueryGroup IDs to their corresponding resource usage
 * views, a set of active QueryGroups, and a task selection strategy. These components collectively facilitate the
 * identification and cancellation of tasks that threaten to breach QueryGroup resource limits.</p>
 *
 * @see MaximumResourceTaskSelectionStrategy
 * @see QueryGroup
 * @see ResourceType
 */
public class QueryGroupTaskCancellationService {
    public static final double MIN_VALUE = 1e-9;
    private static final Logger log = LogManager.getLogger(QueryGroupTaskCancellationService.class);

    private final WorkloadManagementSettings workloadManagementSettings;
    private final TaskSelectionStrategy taskSelectionStrategy;
    private final QueryGroupResourceUsageTrackerService resourceUsageTrackerService;
    // a map of QueryGroupId to its corresponding QueryGroupLevelResourceUsageView object
    Map<String, QueryGroupLevelResourceUsageView> queryGroupLevelResourceUsageViews;
    private Collection<QueryGroup> activeQueryGroups;
    private Collection<QueryGroup> deletedQueryGroups;
    private Function<String, QueryGroupState> queryGroupStateAccessor;

    public QueryGroupTaskCancellationService(
        WorkloadManagementSettings workloadManagementSettings,
        TaskSelectionStrategy taskSelectionStrategy,
        QueryGroupResourceUsageTrackerService resourceUsageTrackerService
    ) {
        this(workloadManagementSettings, taskSelectionStrategy, resourceUsageTrackerService, new HashSet<>(), new HashSet<>());
    }

    public QueryGroupTaskCancellationService(
        WorkloadManagementSettings workloadManagementSettings,
        TaskSelectionStrategy taskSelectionStrategy,
        QueryGroupResourceUsageTrackerService resourceUsageTrackerService,
        Collection<QueryGroup> activeQueryGroups,
        Collection<QueryGroup> deletedQueryGroups
    ) {
        this.workloadManagementSettings = workloadManagementSettings;
        this.taskSelectionStrategy = taskSelectionStrategy;
        this.resourceUsageTrackerService = resourceUsageTrackerService;
        this.activeQueryGroups = activeQueryGroups;
        this.deletedQueryGroups = deletedQueryGroups;
    }

    public void setQueryGroupStateMapAccessor(final Function<String, QueryGroupState> queryGroupStateAccessor) {
        this.queryGroupStateAccessor = queryGroupStateAccessor;
    }

    /**
     * Cancel tasks based on the implemented strategy.
     */
    public void cancelTasks(BooleanSupplier isNodeInDuress) {
        queryGroupLevelResourceUsageViews = resourceUsageTrackerService.constructQueryGroupLevelUsageViews();
        // cancel tasks from QueryGroups that are in Enforced mode that are breaching their resource limits
        cancelTasks(ResiliencyMode.ENFORCED);
        // if the node is in duress, cancel tasks accordingly.
        handleNodeDuress(isNodeInDuress);

        updateResourceUsageInQueryGroupState();
    }

    private void updateResourceUsageInQueryGroupState() {
        for (Map.Entry<String, QueryGroupLevelResourceUsageView> queryGroupLevelResourceUsageViewEntry : queryGroupLevelResourceUsageViews
            .entrySet()) {
            QueryGroupState queryGroupState = getQueryGroupState(queryGroupLevelResourceUsageViewEntry.getKey());
            TRACKED_RESOURCES.forEach(resourceType -> {
                final double currentUsage = queryGroupLevelResourceUsageViewEntry.getValue().getResourceUsageData().get(resourceType);
                queryGroupState.getResourceState().get(resourceType).setLastRecordedUsage(currentUsage);
            });
        }
    }

    private void handleNodeDuress(BooleanSupplier isNodeInDuress) {
        if (!isNodeInDuress.getAsBoolean()) {
            return;
        }
        // List of tasks to be executed in order if the node is in duress
        List<Consumer<Void>> duressActions = List.of(v -> cancelTasksFromDeletedQueryGroups(), v -> cancelTasks(ResiliencyMode.SOFT));

        for (Consumer<Void> duressAction : duressActions) {
            if (!isNodeInDuress.getAsBoolean()) {
                break;
            }
            duressAction.accept(null);
        }
    }

    private void cancelTasksFromDeletedQueryGroups() {
        cancelTasks(getAllCancellableTasks(this.deletedQueryGroups));
    }

    /**
     * Get all cancellable tasks from the QueryGroups.
     *
     * @return List of tasks that can be cancelled
     */
    List<TaskCancellation> getAllCancellableTasks(ResiliencyMode resiliencyMode) {
        return getAllCancellableTasks(
            activeQueryGroups.stream().filter(queryGroup -> queryGroup.getResiliencyMode() == resiliencyMode).collect(Collectors.toList())
        );
    }

    /**
     * Get all cancellable tasks from the given QueryGroups.
     *
     * @return List of tasks that can be cancelled
     */
    List<TaskCancellation> getAllCancellableTasks(Collection<QueryGroup> queryGroups) {
        List<TaskCancellation> taskCancellations = new ArrayList<>();
        final List<Runnable> onCancelCallbacks = new ArrayList<>();
        for (QueryGroup queryGroup : queryGroups) {
            final List<TaskCancellation.Reason> reasons = new ArrayList<>();
            List<QueryGroupTask> selectedTasks = new ArrayList<>();
            for (ResourceType resourceType : TRACKED_RESOURCES) {
                // We need to consider the already selected tasks since those tasks also consumed the resources
                double excessUsage = getExcessUsage(queryGroup, resourceType) - resourceType.getResourceUsageCalculator()
                    .calculateResourceUsage(selectedTasks);
                if (excessUsage > MIN_VALUE) {
                    reasons.add(new TaskCancellation.Reason(generateReasonString(queryGroup, resourceType), 1));

                    // TODO: We will need to add the cancellation callback for these resources for the queryGroup to reflect stats
                    onCancelCallbacks.add(this.getResourceTypeOnCancelCallback(queryGroup.get_id(), resourceType));
                    // Only add tasks not already added to avoid double cancellations
                    selectedTasks.addAll(
                        taskSelectionStrategy.selectTasksForCancellation(getTasksFor(queryGroup), excessUsage, resourceType)
                            .stream()
                            .filter(x -> selectedTasks.stream().noneMatch(y -> x.getId() != y.getId()))
                            .collect(Collectors.toList())
                    );
                }
            }

            if (!reasons.isEmpty()) {
                onCancelCallbacks.add(getQueryGroupState(queryGroup.get_id()).totalCancellations::inc);
                taskCancellations.addAll(
                    selectedTasks.stream().map(task -> new TaskCancellation(task, reasons, onCancelCallbacks)).collect(Collectors.toList())
                );
            }
        }
        return taskCancellations;
    }

    private String generateReasonString(QueryGroup queryGroup, ResourceType resourceType) {
        final double currentUsage = getCurrentUsage(queryGroup, resourceType);
        return "QueryGroup ID : "
            + queryGroup.get_id()
            + " breached the resource limit: ("
            + currentUsage
            + " > "
            + queryGroup.getResourceLimits().get(resourceType)
            + ") for resource type : "
            + resourceType.getName();
    }

    private List<QueryGroupTask> getTasksFor(QueryGroup queryGroup) {
        return queryGroupLevelResourceUsageViews.get(queryGroup.get_id()).getActiveTasks();
    }

    private void cancelTasks(ResiliencyMode resiliencyMode) {
        cancelTasks(getAllCancellableTasks(resiliencyMode));
    }

    private void cancelTasks(List<TaskCancellation> cancellableTasks) {
        cancellableTasks.forEach(TaskCancellation::cancel);
    }

    private double getExcessUsage(QueryGroup queryGroup, ResourceType resourceType) {
        if (queryGroup.getResourceLimits().get(resourceType) == null
            || !queryGroupLevelResourceUsageViews.containsKey(queryGroup.get_id())) {
            return 0;
        }
        return getCurrentUsage(queryGroup, resourceType) - getNormalisedThreshold(queryGroup, resourceType);
    }

    private double getCurrentUsage(QueryGroup queryGroup, ResourceType resourceType) {
        final QueryGroupLevelResourceUsageView queryGroupResourceUsageView = queryGroupLevelResourceUsageViews.get(queryGroup.get_id());
        return queryGroupResourceUsageView.getResourceUsageData().get(resourceType);
    }

    /**
     * normalises configured value with respect to node level cancellation thresholds
     * @param queryGroup instance
     * @return normalised value with respect to node level cancellation thresholds
     */
    private double getNormalisedThreshold(QueryGroup queryGroup, ResourceType resourceType) {
        double nodeLevelCancellationThreshold = resourceType.getNodeLevelThreshold(workloadManagementSettings);
        return queryGroup.getResourceLimits().get(resourceType) * nodeLevelCancellationThreshold;
    }

    private Runnable getResourceTypeOnCancelCallback(String queryGroupId, ResourceType resourceType) {
        QueryGroupState queryGroupState = getQueryGroupState(queryGroupId);
        return queryGroupState.getResourceState().get(resourceType).cancellations::inc;
    }

    private QueryGroupState getQueryGroupState(String queryGroupId) {
        assert queryGroupId != null : "queryGroupId should never be null at this point.";

        return queryGroupStateAccessor.apply(queryGroupId);
    }

    public void refreshQueryGroups(Collection<QueryGroup> activeQueryGroups, Collection<QueryGroup> deletedQueryGroups) {
        this.activeQueryGroups = activeQueryGroups;
        this.deletedQueryGroups = deletedQueryGroups;
    }
}
