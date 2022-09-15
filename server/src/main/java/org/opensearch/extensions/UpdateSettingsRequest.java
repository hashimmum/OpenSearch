/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * Update Settings Request for Extensibility
 *
 * @opensearch.internal
 */
public class UpdateSettingsRequest extends TransportRequest {
    private static final Logger logger = LogManager.getLogger(EnvironmentSettingsRequest.class);

    private Setting<?> componentSetting;
    private Object data;

    public UpdateSettingsRequest(Setting<?> componentSetting, Object data) {
        this.componentSetting = componentSetting;
        this.data = data;
    }

    public UpdateSettingsRequest(StreamInput in) throws IOException {
        super(in);
        // TODO : After getSetting support is added, replace
        // this.componentSetting = new WriteableSetting(in);
        this.componentSetting = null;
        this.data = in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        // TODO : After getSetting support is added, uncomment
        // new WriteableSetting(componentSetting).writeTo(out);
        out.writeGenericValue(this.data);
    }

    public Setting<?> getComponentSetting() {
        return this.componentSetting;
    }

    public Object getData() {
        return this.data;
    }

    @Override
    public String toString() {
        return "UpdateSettingRequest{componentSetting=" + this.componentSetting.toString() + ", data=" + this.data.toString() + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        UpdateSettingsRequest that = (UpdateSettingsRequest) obj;
        return Objects.equals(componentSetting, that.componentSetting) && Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(componentSetting, data);
    }

}
