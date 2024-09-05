/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.MutableQueryGroupFragment;
import org.opensearch.wlm.MutableQueryGroupFragment.ResiliencyMode;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.tracker.ResourceUsageCalculatorTrackerServiceTests.TestClock;

import java.util.List;
import java.util.Map;

import static org.opensearch.wlm.cancellation.TaskCanceller.MIN_VALUE;
import static org.opensearch.wlm.tracker.CpuUsageCalculator.PROCESSOR_COUNT;
import static org.opensearch.wlm.tracker.MemoryUsageCalculator.HEAP_SIZE_BYTES;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResourceUsageCalculatorTests extends OpenSearchTestCase {
    ResourceUsageCalculator sut;

    public void testFactoryMethods() {
        ResourceUsageCalculatorFactory resourceUsageCalculatorFactory = ResourceUsageCalculatorFactory.getInstance();
        assertTrue(resourceUsageCalculatorFactory.getInstanceForResourceType(ResourceType.CPU) instanceof CpuUsageCalculator);
        assertTrue(resourceUsageCalculatorFactory.getInstanceForResourceType(ResourceType.MEMORY) instanceof MemoryUsageCalculator);
        assertThrows(IllegalArgumentException.class, () -> resourceUsageCalculatorFactory.getInstanceForResourceType(null));
    }

    public void testQueryGroupCpuUsage() {
        sut = CpuUsageCalculator.getInstance();
        TestClock clock = new TestClock();
        long fastForwardTime = PROCESSOR_COUNT * 200L;
        clock.fastForwardBy(fastForwardTime);
        QueryGroup queryGroup = new QueryGroup(
            "testQG",
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5 / PROCESSOR_COUNT))
        );
        double expectedQueryGroupCpuUsage = 1.0 / PROCESSOR_COUNT;

        QueryGroupTask mockTask = createMockTaskWithResourceStats(QueryGroupTask.class, fastForwardTime, 200, 0, 123);
        double actualUsage = sut.calculateResourceUsage(List.of(mockTask), clock::getTime);
        assertEquals(expectedQueryGroupCpuUsage, actualUsage, MIN_VALUE);

        double taskResourceUsage = sut.calculateTaskResourceUsage(mockTask, clock::getTime);
        assertEquals(1.0, taskResourceUsage, MIN_VALUE);
    }

    public void testQueryGroupMemoryUsage() {
        sut = MemoryUsageCalculator.getInstance();
        TestClock clock = new TestClock();

        QueryGroupTask mockTask = createMockTaskWithResourceStats(QueryGroupTask.class, 100, 200, 0, 123);
        double actualMemoryUsage = sut.calculateResourceUsage(List.of(mockTask), clock::getTime);
        double expectedMemoryUsage = 200.0 / HEAP_SIZE_BYTES;

        assertEquals(expectedMemoryUsage, actualMemoryUsage, MIN_VALUE);
        assertEquals(200.0 / HEAP_SIZE_BYTES, sut.calculateTaskResourceUsage(mockTask, clock::getTime), MIN_VALUE);
    }

    public static <T extends QueryGroupTask> T createMockTaskWithResourceStats(
        Class<T> type,
        long cpuUsage,
        long heapUsage,
        long startTimeNanos,
        long taskId
    ) {
        T task = mock(type);
        when(task.getTotalResourceUtilization(ResourceStats.CPU)).thenReturn(cpuUsage);
        when(task.getTotalResourceUtilization(ResourceStats.MEMORY)).thenReturn(heapUsage);
        when(task.getStartTimeNanos()).thenReturn(startTimeNanos);
        when(task.getId()).thenReturn(taskId);
        return task;
    }
}
