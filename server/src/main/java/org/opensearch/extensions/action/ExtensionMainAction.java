/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.action.ActionType;

/**
 * The main proxy action for all extensions
 *
 * @opensearch.internal
 */
public class ExtensionMainAction extends ActionType<ExtensionActionResponse> {
    public static final String NAME = "cluster:internal/extension";
    public static final ExtensionMainAction INSTANCE = new ExtensionMainAction();

    public ExtensionMainAction() {
        super(NAME, ExtensionActionResponse::new);
    }
}
