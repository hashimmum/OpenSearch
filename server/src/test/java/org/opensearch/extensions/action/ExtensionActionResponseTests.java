/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;

public class ExtensionActionResponseTests extends OpenSearchTestCase {

    public void testExtensionActionResponse() throws Exception {
        byte[] expectedResponseBytes = "response-bytes".getBytes(StandardCharsets.UTF_8);
        ExtensionActionResponse response = new ExtensionActionResponse(true, expectedResponseBytes);

        assertTrue(response.isSuccess());
        assertEquals(expectedResponseBytes, response.getResponseBytes());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        response = new ExtensionActionResponse(in);

        assertTrue(response.isSuccess());
        assertArrayEquals(expectedResponseBytes, response.getResponseBytes());
    }

    public void testSetters() {
        String expectedResponse = "response-bytes";
        byte[] expectedResponseBytes = expectedResponse.getBytes(StandardCharsets.UTF_8);
        byte[] expectedEmptyBytes = new byte[0];
        ExtensionActionResponse response = new ExtensionActionResponse(false, expectedEmptyBytes);
        assertArrayEquals(expectedEmptyBytes, response.getResponseBytes());
        assertFalse(response.isSuccess());

        response.setResponseBytesAsString(expectedResponse);
        assertArrayEquals(expectedResponseBytes, response.getResponseBytes());

        response.setResponseBytes(expectedResponseBytes);
        assertArrayEquals(expectedResponseBytes, response.getResponseBytes());
        assertEquals(expectedResponse, response.getResponseBytesAsString());

        response.setSuccess(true);
        assertTrue(response.isSuccess());
    }
}
