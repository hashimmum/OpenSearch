/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.settings;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.extensions.ExtensionStringResponse;
import org.opensearch.transport.TransportResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles requests to register extension settings.
 *
 * @opensearch.internal
 */
public class SettingsRequestHandler {

    private final SettingsModule settingsModule;

    /**
     * Instantiates a new Settings Request Handler using the Node's SettingsModule.
     *
     * @param settingsModule  The Node's {@link SettingsModule}.
     */
    public SettingsRequestHandler(SettingsModule settingsModule) {
        this.settingsModule = settingsModule;
    }

    /**
     * Handles a {@link RegisterSettingsRequest}.
     *
     * @param settingsRequest  The request to handle.
     * @return A {@link ExtensionStringResponse} indicating success.
     * @throws Exception if the request is not handled properly.
     */
    public TransportResponse handleRegisterSettingsRequest(RegisterSettingsRequest settingsRequest) throws Exception {
        // TODO: How do we prevent key collisions in settings registration?
        // we have settingsRequest.getUniqueId() available or could enforce reverse DNS naming
        // See https://github.com/opensearch-project/opensearch-sdk-java/issues/142
        List<String> registeredSettings = new ArrayList<>();
        for (Setting<?> setting : settingsRequest.getSettings()) {
            settingsModule.registerDynamicSetting(setting);
            registeredSettings.add(setting.getKey());
        }
        return new ExtensionStringResponse(
            "Registered settings from extension " + settingsRequest.getUniqueId() + ": " + String.join(", ", registeredSettings)
        );
    }
}
