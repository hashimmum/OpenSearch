/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offline_tasks;

/**
 * A Background Task to be run on Offline Node.
 */
public class Task {

    /**
     * Task identifier used to uniquely identify a Task
     */
    private TaskId taskId;

    /**
     * Various params to used for Task execution
     */
    private TaskParams params;

    /**
     * Type/Category of the Task
     */
    private TaskType taskType;

    /**
     * Constructor for Task
     *
     * @param taskId Task identifier
     * @param params Task Params
     * @param taskType Task Type
     */
    public Task(TaskId taskId, TaskParams params, TaskType taskType) {
        this.taskId = taskId;
        this.params = params;
        this.taskType = taskType;
    }
}
