/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.controllers;

/**
 * Interface for Admission Controller in OpenSearch, which aims to provide resource based request admission control.
 * It provides methods for any tracking-object that can be incremented (such as memory size),
 * and admission control can be applied if configured limit has been reached
 */
public interface AdmissionController {

    /**
     * Return the current state of the admission controller
     * @return true if admissionController is enabled for the transport layer else false
     */
    boolean isEnabledForTransportLayer();

    /**
     * Increment the tracking-objects and apply the admission control if threshold is breached.
     * Mostly applicable while applying admission controller
     */
    void apply(String action);

    /**
     * @return name of the admission-controller
     */
    String getName();

    /**
     * Adds the rejection count for the controller. Primarily used when copying controller states.
     * @param count To add the value of the tracking resource object as the provided count
     */
    void addRejectionCount(long count);

    /**
     * @return current value of the rejection count metric tracked by the admission-controller.
     */
    long getRejectionCount();
}
