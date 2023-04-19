/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro.realm;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authc.pam.UnsupportedTokenException;
import org.apache.shiro.realm.AuthenticatingRealm;
import org.apache.shiro.authc.UsernamePasswordToken;

import org.opensearch.identity.NamedPrincipal;

import java.util.Objects;
import java.util.Map;
import java.util.Optional;

/**
 * Internal Realm is a custom realm using the internal OpenSearch IdP
 *
 * @opensearch.experimental
 */
public class OpenSearchRealm extends AuthenticatingRealm {
    private static final String DEFAULT_REALM_NAME = "internal";

    public static final OpenSearchRealm INSTANCE = new OpenSearchRealm.Builder(DEFAULT_REALM_NAME).build();

    private final String realmName;

    private Map<String, User> internalUsers;

    private OpenSearchRealm(final String realmName, final Map<String, User> internalUsers) {
        super(new BCryptPasswordMatcher());
        this.realmName = realmName;
        this.internalUsers = internalUsers;
        setAuthenticationTokenClass(UsernamePasswordToken.class);
    }

    public static final class Builder {
        private final String name;

        public Builder(final String name) {
            this.name = Objects.requireNonNull(name);
        }

        public OpenSearchRealm build() {
            // TODO: Replace hardcoded admin user / user map with an external provider
            final User adminUser = new User();
            adminUser.setUsername(new NamedPrincipal("admin"));
            adminUser.setBcryptHash("$2a$12$VcCDgh2NDk07JGN0rjGbM.Ad41qVR/YFJcgHp0UGns5JDymv..TOG"); // Password 'admin'
            final Map<String, User> internalUsers = Map.of("admin", adminUser);
            return new OpenSearchRealm(name, internalUsers);
        }
    }

    public User getInternalUser(final String principalIdentifier) throws UnknownAccountException {
        final User userRecord = internalUsers.get(principalIdentifier);
        if (userRecord == null) {
            throw new UnknownAccountException();
        }
        return userRecord;
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(final AuthenticationToken token) throws AuthenticationException {
        if (token instanceof UsernamePasswordToken) {
            final String username = ((UsernamePasswordToken) token).getUsername();
            // Look up the user by the provide username
            final User userRecord = getInternalUser(username);
            // TODO: Check for other things, like a locked account, expired password, etc.

            // Verify the user
            final SimpleAuthenticationInfo sai = new SimpleAuthenticationInfo(
                userRecord.getUsername(),
                userRecord.getBcryptHash(),
                realmName
            );

            final boolean successfulAuthentication = getCredentialsMatcher().doCredentialsMatch(token, sai);

            if (successfulAuthentication) {
                // TODO: Check for anything else that might prevent login (expired password, locked account, etc.)
                // if (other problems) {
                // throw new CredentialsException(); // Or something more specific
                // }
                // Success!
                return sai;
            } else {
                // Bad password
                throw new IncorrectCredentialsException();
            }
        }

        // If the token was not handled, it was unsupported
        final String tokenClassName = Optional.ofNullable(token).map(Object::getClass).map(Class::getName).orElse("null");
        throw new UnsupportedTokenException("Unable to support authentication token " + tokenClassName);
    }
}
