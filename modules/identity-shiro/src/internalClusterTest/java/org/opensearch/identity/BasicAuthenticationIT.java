/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.rest.RestStatus;

import java.nio.charset.StandardCharsets;
import static org.hamcrest.Matchers.equalTo;

import static org.hamcrest.core.StringContains.containsString;

public class BasicAuthenticationIT extends HttpSmokeTestCaseWithIdentity {

    public void testBasicAuthSuccess() throws Exception {
        final Response response = createHealthRequest("Basic YWRtaW46YWRtaW4="); // admin:admin
        final String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        assertThat(content, response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        assertThat(content, containsString("green"));
    }

    public void testBasicAuthUnauthorized_invalidHeader() throws Exception {
        final Response response = createHealthRequest("Basic aaaa"); // invalid username password
        final String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        assertThat(content, response.getStatusLine().getStatusCode(), equalTo(RestStatus.UNAUTHORIZED.getStatus()));
        assertThat(content, containsString("Illegally formed basic authorization header"));
    }

    public void testBasicAuthUnauthorized_wrongPassword() throws Exception {
        final Response response = createHealthRequest("Basic YWRtaW46aW52YWxpZFBhc3N3b3Jk"); // admin:invalidPassword
        final String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        assertThat(content, response.getStatusLine().getStatusCode(), equalTo(RestStatus.UNAUTHORIZED.getStatus()));
    }

    public void testBasicAuthUnauthorized_unknownUser() throws Exception {
        final Response response = createHealthRequest("Basic dXNlcjpkb2VzTm90RXhpc3Q="); // user:doesNotExist
        final String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        assertThat(content, response.getStatusLine().getStatusCode(), equalTo(RestStatus.UNAUTHORIZED.getStatus()));
    }

    private Response createHealthRequest(final String authorizationHeaderValue) throws Exception {
        final Request request = new Request("GET", "/_cluster/health");
        final RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authorizationHeaderValue).build();
        request.setOptions(options);

        try {
            final Response response = getRestClient().performRequest(request);
            return response;
        } catch (final ResponseException re) {
            return re.getResponse();
        }
    }
}
