/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.core.action.ActionListener;

public class TestCapturingListener<T> implements ActionListener<T> {
    T result;
    Exception failure;

    @Override
    public void onResponse(T result) {
        this.result = result;
    }

    @Override
    public void onFailure(Exception e) {
        this.failure = e;
    }

    public T getResult() {
        return result;
    }

    public Exception getFailure() {
        return failure;
    }
}
