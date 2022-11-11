/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.jwt;

import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.shiro.authc.AuthenticationToken;
import org.opensearch.authn.AccessToken;
import org.opensearch.authn.HttpHeaderToken;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.identity.AuthenticationTokenHandler;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.identity.AuthenticationTokenHandler.extractShiroAuthToken;

public class BearerAuthTests extends OpenSearchTestCase {

    public void testExpiredValidJwt() {
        Map<String, Object> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");

        String encodedToken = JwtVendor.createJwt(jwtClaims);

        //TODO: Not sure whether this is the proper way to test the HTTP Extraction and Verification but does not look like there is another way right now
        // When testing with Invalid JWT need to create it with one sign in key but verify with another
        // Will need to add more and more integration tests -- can create token for now and then just unit test -- create bearer token with one of the jwts and then just test the unit
        // can then move up a step and test handleBearerAuth

        String headerBody = "Bearer " + encodedToken;
        HttpHeaderToken HttpToken = new HttpHeaderToken(headerBody); // Create an HttpHeaderToken that just holds the 'Bearer' + JWT
        AuthenticationToken extractedShiroToken = extractShiroAuthToken(HttpToken); // This should verify and then extract the shiro token for login
    }

    public void testEarlyValidJwt() {
        Map<String, Object> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");

        String encodedToken = JwtVendor.createJwt(jwtClaims);

        try {
            JwtToken token = JwtVerifier.getVerifiedJwtToken(encodedToken);
            assertTrue(token.getClaims().getClaim("sub").equals("testSubject"));
        } catch (BadCredentialsException e) {
            fail("Unexpected BadCredentialsException thrown");
        }
    }

    public void testValidJwt() {
        Map<String, Object> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");

        String encodedToken = JwtVendor.createJwt(jwtClaims);

        try {
            JwtToken token = JwtVerifier.getVerifiedJwtToken(encodedToken);
            assertTrue(token.getClaims().getClaim("sub").equals("testSubject"));
        } catch (BadCredentialsException e) {
            fail("Unexpected BadCredentialsException thrown");
        }
    }

    public void testInvalidJwt() {
        Map<String, Object> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");

        String encodedToken = JwtVendor.createJwt(jwtClaims);

        try {
            JwtToken token = JwtVerifier.getVerifiedJwtToken(encodedToken);
            assertTrue(token.getClaims().getClaim("sub").equals("testSubject"));
        } catch (BadCredentialsException e) {
            fail("Unexpected BadCredentialsException thrown");
        }
    }
}
