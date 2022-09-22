/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * This class encapsulates a transport request to extension
 *
 * @opensearch.api
 */
public class ExtensionHandleTransportRequest extends TransportRequest {
    private final String action;
    private final byte[] requestBytes;

    public ExtensionHandleTransportRequest(String action, byte[] requestBytes) {
        this.action = action;
        this.requestBytes = requestBytes;
    }

    public ExtensionHandleTransportRequest(StreamInput in) throws IOException {
        super(in);
        this.action = in.readString();
        this.requestBytes = in.readByteArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(action);
        out.writeByteArray(requestBytes);
    }

    @Override
    public String toString() {
        return "ExtensionHandleTransportRequest{action=" + action + ", requestBytes=" + requestBytes + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ExtensionHandleTransportRequest that = (ExtensionHandleTransportRequest) obj;
        return Objects.equals(action, that.action) && Objects.equals(requestBytes, that.requestBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(action, requestBytes);
    }

}
