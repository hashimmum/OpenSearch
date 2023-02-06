/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.jwt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IdentityJwtVerifier extends AbstractJwtVerifier {
    private final static Logger log = LogManager.getLogger(JwtVerifier.class);

    private static IdentityJwtVerifier INSTANCE;

    private IdentityJwtVerifier() {}

    public static IdentityJwtVerifier getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new IdentityJwtVerifier();
        }

        return INSTANCE;
    }

    public void init(String signingKey) {
        if (this.signingKey == null) {
            this.signingKey = signingKey;
        }
    }
}
